import asyncio
import csv
import io
import json
import uuid
from datetime import datetime
from logging import getLogger
from os import environ
from typing import Annotated, Literal, List, Dict, Any, Optional

from asyncpg import UniqueViolationError
from fastapi import HTTPException
from pytz import timezone as pytz_timezone, UnknownTimeZoneError
from uuid import UUID

logger = getLogger(__name__)

from app.database import get_pg_readonly_database, get_pg_database
from app.model.ams_models import (
    AccountRequestModelV2,
    CreateStep,
    PersonRequestModel,
    AddressRequestModel,
    EmailRequestModel,
    AccountRequestModel,
    PhoneNumberRequestModel,
    AcctStepRequestModel,
    ProxyRequestModel,
    AccountPrimary,
    Primary,
    EmailTwoFARequestModel,
    EmailTwoFAResponseModel, EmailCommonFieldsRequest, RebuildAccountRequestItem,
)
from app.service.ticketsuite.ts_credential_manager import ts_credential_manager
from app.service.geocode_ams_address import geocode_ams_address
from app.service.ticketsuite.ts_persona_client import get_ticketsuite_persona_client
from app.service.ticketsuite.utils.ticketsuite_models import TsProxyPayload
from app.service.vaultwarden_service import VaultwardenRegistration
from app.service.password_service import PasswordGenerator
from app.utils import haversine_distance

CONCURRENCY_LIMIT = 20

async def get_searched_persons(
        page: int = 1,
        page_size: int = 10,
        sort_field: str | None = None,
        sort_order: str = 'desc',
        search_query: str = "",
        metro: Literal["all", "yes", "no"] = "all",
        name_quality: Literal["all", "Employee", "Non-Employee", "Contractor", "Department"] = "all",
        status: Literal["all", "Active", "Inactive", "Active - No New Accts"] = "all",
        timezone: str = "America/Chicago"
):
    offset = (page - 1) * page_size
    # Default sorting if isn't specified
    if not sort_field:
        sort_field = "last_modified"

    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    # Validate sort_field to prevent SQL injection
    valid_columns = ["first_name", "last_name", "full_name", "date_of_birth",
                     "created_at", "last_modified", "metro_count", "is_starred", "status",
                     "name_quality", "account_info"]

    if sort_field not in valid_columns:
        sort_field = "last_modified"  # Default to a safe column

    # Special handling for metro_count because it's calculated differently
    if sort_field == "metro_count":
        order_clause = f"metro_count {sort_order}"
    elif sort_field == "account_info":
        order_clause = f"json_array_length(COALESCE(account_info, '[]'::json)) {sort_order}"
    else:
        order_clause = f"cp.{sort_field} {sort_order}"

    if not timezone:
        timezone = "America/Chicago"

    try:
        timezone = pytz_timezone(timezone)
    except UnknownTimeZoneError:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown timezone: {timezone}. Please provide a valid timezone string."
        )
    try:
        query = f"""
             WITH filtered_persons AS (
                SELECT
                    p.id,
                    p.first_name,
                    p.last_name,
                    p.date_of_birth,
                    p.status,
                    p.last_4_ssn,
                    p.full_name,
                    p.notes,
                    p.name_quality,
                    p.created_at,
                    p.is_starred,
                    COUNT(DISTINCT cc.id) AS cards,
                    p.last_modified AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_modified,
                    json_agg(
                        json_build_object(
                            'id', a.id,
                            'nickname', a.nickname,
                            'status', a.status_code
                        )
                    ) FILTER (WHERE a.id IS NOT NULL) AS account_info
                FROM
                    ams.ams_person p
                LEFT JOIN
                    ams.ams_account a ON p.id = a.ams_person_id
                LEFT JOIN ams.ams_credit_card cc ON cc.ams_person_id = p.id
                WHERE
                    (p.first_name || ' ' || p.last_name || ' ' || p.full_name || COALESCE(p.id::text, '')) ILIKE '%' || :search_query || '%'
                    AND CASE
                        WHEN :metro = 'all' THEN TRUE
                        WHEN :metro = 'yes' THEN a.ams_person_id IS NOT NULL
                        WHEN :metro = 'no' THEN a.ams_person_id IS NULL
                        ELSE TRUE
                    END
                    AND p.name_quality = COALESCE(CAST(:name_quality AS ams.name_quality_type), p.name_quality)
                    AND p.status       = COALESCE(CAST(:status AS text),                         p.status)
                GROUP BY
                    p.id,
                    p.first_name,
                    p.last_name,
                    p.date_of_birth,
                    p.status,
                    p.last_4_ssn,
                    p.full_name,
                    p.notes,
                    p.name_quality
            ),
            counted_persons AS (
                SELECT *, COUNT(*) OVER () AS total_count
                FROM filtered_persons
            )
            SELECT
                cp.*,
                (SELECT COUNT(*) FROM ams.ams_account WHERE ams_person_id = cp.id) AS metro_count
            FROM
                counted_persons cp
            ORDER BY
                {order_clause}
            LIMIT :page_size OFFSET :offset;
            """
        result = await get_pg_readonly_database().fetch_all(
            query=query, values={
                "search_query": search_query,
                "page_size": page_size,
                "offset": offset,
                "metro": metro,
                "name_quality": name_quality if name_quality not in (None, "all") else None,
                "status": status if status not in (None, "all") else None,
                "timezone": timezone.zone
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_persons():
    try:
        query = """
            SELECT * FROM ams.ams_person
            WHERE ams_person.status='Active'
            ORDER BY last_modified DESC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_recovery_emails():
    try:
        query = """
            SELECT
                  id
                , email_address
            FROM
                ams.ams_recovery_email
            WHERE
                status = 'useable';
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_recovery_phones():
    try:
        query = """
            SELECT 
                rp.id, ph.number, ph.status
            FROM ams.ams_recovery_phone rp
            JOIN ams.phone_number ph ON rp.phone_number_id = ph.id
            WHERE rp.status = 'Available'
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_forwarding_emails():
    try:
        query = """
            SELECT id, email_address, created_at, last_modified
            FROM ams.ams_forwarding_email
            ORDER BY email_address
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_catchall_emails():
    try:
        query = """
            SELECT id, email_address, created_at, last_modified
            FROM ams.ams_catchall_email
            ORDER BY email_address
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_phone_numbers(status_list: List[str]):
    try:
        query = """
            SELECT 
                id, number, status
            FROM ams.phone_number
            WHERE status::VARCHAR = ANY(:status_list)
            ORDER BY last_modified ASC
        """

        return await get_pg_readonly_database().fetch_all(
            query=query,
            values={"status_list": status_list}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_person(person_id: str):
    try:
        query = """
            SELECT *
            FROM ams.ams_person
            WHERE id = :person_id
            """
        result = await get_pg_readonly_database().fetch_one(
            query=query, values={"person_id": person_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_persons_status(update_data: dict[str, list[str] | str]):
    try:
        # Get the list of person IDs
        person_ids = update_data['person_ids']

        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(person_ids))])

        query = f"""
        UPDATE ams.ams_person
        SET
            status = :status,
            last_modified = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        RETURNING id;
        """

        # Create values dict with individual ID parameters
        values = {"status": update_data['status']}
        for i, pid in enumerate(person_ids):
            values[f"id{i}"] = pid

        # Update persons' status
        result = await get_pg_database().fetch_all(
            query=query,
            values=values
        )

        # If the status is "Inactive", deactivate all linked accounts
        if update_data['status'] == "Inactive":
            acct_values = {"status_code": "INACTIVE_SUNSETTING"}
            for i, pid in enumerate(person_ids):
                acct_values[f"id{i}"] = pid

            account_query = f"""
            UPDATE ams.ams_account a
            SET
                status_code   = :status_code,
                status_id     = s.id,
                last_modified = CURRENT_TIMESTAMP
            FROM ams.ams_account_status s
            WHERE a.status_code = 'ACTIVE' AND s.code = :status_code
              AND ams_person_id IN ({placeholders})
            RETURNING a.id;
            """
            await get_pg_database().fetch_all(
                query=account_query,
                values=acct_values
            )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_persons_star(person_id: str):
    try:
        query = """
        UPDATE ams.ams_person
        SET
            is_starred = NOT is_starred
        WHERE id = :person_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"person_id": person_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_person(person_id: str, person: PersonRequestModel):
    try:
        query = """
            UPDATE ams.ams_person
            SET 
                first_name = :first_name,
                last_name = :last_name,
                date_of_birth = :date_of_birth,
                status = :status,
                name_quality = :name_quality,
                last_4_ssn = :last_4_ssn,
                notes = :notes,
                full_name = :full_name, 
                last_modified = CURRENT_TIMESTAMP
            WHERE id = :person_id
            RETURNING *;
            """
        values = {
            **person.model_dump(),
            "date_of_birth": datetime.fromisoformat(
                person.date_of_birth.replace("Z", "")) if person.date_of_birth else None,
            "person_id": person_id
        }
        result = await get_pg_database().fetch_one(query=query, values=values)

        # If the status is "Inactive", deactivate all linked accounts
        if person.model_dump()['status'] == "Inactive":

            account_query = """
            UPDATE ams.ams_account a
            SET
                status_code   = :status_code,
                status_id     = s.id,
                last_modified = CURRENT_TIMESTAMP
            FROM ams.ams_account_status s
            WHERE a.status_code='ACTIVE' AND s.code = :status_code
              AND ams_person_id = :person_id
            RETURNING a.id;
            """
            await get_pg_database().fetch_all(
                query=account_query,
                values={"status_code": "INACTIVE_SUNSETTING", "person_id": person_id}
            )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_persons(persons: list[PersonRequestModel]):
    try:
        query = """
            INSERT INTO ams.ams_person (id, first_name, last_name, 
            date_of_birth, status, name_quality, last_4_ssn, notes, full_name)
            VALUES (:id, :first_name, :last_name, :date_of_birth, 
            :status, :name_quality, :last_4_ssn, :notes, :full_name)
        """
        values = [
            {
                "id": str(uuid.uuid4()),
                "first_name": person.first_name,
                "last_name": person.last_name,
                "date_of_birth": datetime.fromisoformat(
                    person.date_of_birth.replace("Z", "")) if person.date_of_birth else None,
                "status": person.status,
                "name_quality": person.name_quality,
                "last_4_ssn": person.last_4_ssn,
                "notes": person.notes,
                "full_name": person.full_name
            }
            for person in persons
        ]
        await get_pg_database().execute_many(query=query, values=values)
        return {"inserted_count": len(persons), "message": "Persons created successfully."}
    except UniqueViolationError:  # Handle unique constraint violation
        raise HTTPException(status_code=400, detail="A person with these details already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_metro_areas():
    try:
        query = """
            SELECT
                m.id,
                m.name,
                m.timezone,
                s.abbreviation AS state,
                c.name AS country,
                COUNT(a.id) AS address_count
            FROM ams.metro_area m
            LEFT JOIN ams.state s ON m.state_id = s.id
            LEFT JOIN ams.country c ON m.country_id = c.id
            LEFT JOIN ams.ams_address a ON m.id = a.metro_area_id
            GROUP BY m.id, m.name, s.abbreviation, c.name
            ORDER BY m.name ASC
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_metro_areas_with_available_addresses():
    try:
        query = """
            SELECT
                m.id,
                m.name,
                m.timezone,
                s.abbreviation AS state,
                c.name AS country,
                COUNT(DISTINCT a.id) AS total_address_count,
                COUNT(DISTINCT a.id) FILTER (WHERE ac.id IS NULL) AS unassigned_address_count
            FROM ams.metro_area m
            LEFT JOIN ams.state s ON m.state_id = s.id
            LEFT JOIN ams.country c ON m.country_id = c.id
            LEFT JOIN ams.ams_address a ON m.id = a.metro_area_id
            LEFT JOIN ams.ams_account ac ON a.id = ac.ams_address_id
            GROUP BY m.id, m.name, s.abbreviation, c.name
            ORDER BY m.name ASC;
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

async def get_all_timezones():
    try:
        query = """
            SELECT * FROM ams.timezone
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_states():
    try:
        query = """
            SELECT * FROM ams.state
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_companies():
    try:
        query = """
            SELECT id, name FROM ams.company
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_searched_addresses(
        page: int = 1,
        page_size: int = 10,
        sort_field: Optional[str] = None,
        sort_order: Optional[str] = 'desc',
        search_query: str = "",
        metro_area_ids: Optional[list[str]] = None,
        assigned_to_account: Literal["all", "yes", "no"] = "all",
        timezone: Optional[str] = None,
        address_type: Optional[str] = None
):
    offset = (page - 1) * page_size
    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    # Validate sort_field to prevent SQL injection and handle joins
    sortable_columns = [
        "street_one",
        "street_two",
        "city",
        "state_name",
        "created_at",
        "last_modified",
        "metro_area_name",
        "postal_code",
        "is_starred",
    ]
    if sort_field not in sortable_columns:
        sort_field = "last_modified"  # Default to a safe column
    order_clause = f"{sort_field} {sort_order}"

    try:
        # Build the metro area filter condition
        metro_filter = ""
        if metro_area_ids and len(metro_area_ids) > 0:
            # Create placeholders for each metro area ID
            placeholders = ",".join([f"'{id}'" for id in metro_area_ids])
            metro_filter = f"AND ma.id IN ({placeholders})"

        # Build the address type filter condition
        address_type_filter = ""
        if address_type:
            address_type_filter = f"AND a.address_type = '{address_type}'"

        query = f"""
            WITH base AS (
              SELECT
                a.id, a.street_one, a.street_two, a.city, a.postal_code,
                a.last_modified AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_modified,
                a.created_at, a.lat_long, a.notes, a.shippable, a.is_starred,
                a.address_type, a.address_name,
                jsonb_build_object(
                  'id', ma.id,
                  'name', ma.name,
                  'country', c.name,
                  'state', ms.abbreviation,
                  'timezone', jsonb_build_object(
                    'id', t.id,
                    'name', t.name,
                    'abbreviation', t.abbreviation
                  )
                ) AS metro_area,
                jsonb_build_object(
                  'id', s.id,
                  'name', s.name,
                  'abbreviation', s.abbreviation
                ) AS state,
                ma.name AS metro_area_name,   -- for sorting
                s.abbreviation AS state_name  -- for sorting
              FROM ams.ams_address a
              LEFT JOIN ams.metro_area ma ON a.metro_area_id = ma.id
              LEFT JOIN ams.country c ON ma.country_id = c.id
              LEFT JOIN ams.state ms ON ma.state_id = ms.id
              LEFT JOIN ams.timezone t ON ma.timezone = t.id::TEXT
              LEFT JOIN ams.state s ON a.state_id = s.id
              WHERE
                (COALESCE(a.street_one,'') || ' ' ||
                 COALESCE(a.street_two,'') || ' ' ||
                 COALESCE(a.city,'') || ' ' ||
                 COALESCE(s.abbreviation,'') || ' ' ||
                 COALESCE(a.id::text, '')
                ) ILIKE '%' || :search_query || '%'
                AND (
                  :assigned_to_account = 'all' OR
                  (:assigned_to_account = 'yes' AND EXISTS (
                    SELECT 1 FROM ams.ams_account ac2 WHERE ac2.ams_address_id = a.id
                  )) OR
                  (:assigned_to_account = 'no' AND NOT EXISTS (
                    SELECT 1 FROM ams.ams_account ac3 WHERE ac3.ams_address_id = a.id
                  ))
                )
                {metro_filter}
                {address_type_filter}
            )
            SELECT
              b.*,
              acc.account_ids,           -- array of UUIDs
              acc.account_count,         -- how many accounts
              COUNT(*) OVER () AS total_count
            FROM base b
            LEFT JOIN LATERAL (
              SELECT
                ARRAY_REMOVE(ARRAY_AGG(ac.id ORDER BY ac.id), NULL) AS account_ids,
                COUNT(ac.id) AS account_count
              FROM ams.ams_account ac
              WHERE ac.ams_address_id = b.id
            ) acc ON TRUE
            ORDER BY {order_clause}
            LIMIT :page_size OFFSET :offset;
            """
        result = await get_pg_readonly_database().fetch_all(
            query=query, values={
                "search_query": search_query,
                "page_size": page_size,
                "offset": offset,
                "assigned_to_account": assigned_to_account,
                "timezone": timezone
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_address(address_id: str):
    try:
        query = """
            SELECT 
                a.id, a.street_one, a.street_two, a.city, a.postal_code, a.notes,
                a.address_type, a.address_name, a.shippable,
                jsonb_build_object(
                    'id', ma.id,
                    'name', ma.name,
                    'country', jsonb_build_object(
                        'id', c.id,
                        'name', c.name
                    )
                ) AS metro_area,
                jsonb_build_object(
                    'id', s.id,
                    'name', s.name
                ) AS state
                FROM ams.ams_address AS a
                LEFT JOIN ams.metro_area AS ma ON a.metro_area_id = ma.id
                LEFT JOIN ams.country AS c ON ma.country_id = c.id
                LEFT JOIN ams.state AS s ON a.state_id = s.id
                WHERE a.id = :address_id
                """
        result = await get_pg_readonly_database().fetch_one(query=query, values={"address_id": address_id})
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_addresses_with_filter(address_type: Optional[str] = None):
    """
    Get all addresses with optional filtering by address type.
    
    Args:
        address_type: Optional filter for address type ('Account Address' or 'Billing Address')
    
    Returns:
        List of addresses matching the filter criteria
    """
    try:
        # Build the address type filter condition
        address_type_filter = ""
        if address_type:
            address_type_filter = "WHERE a.address_type = :address_type"
        
        query = f"""
            SELECT 
                a.id, a.street_one, a.street_two, a.city, a.postal_code, 
                a.last_modified, a.created_at, a.lat_long, a.notes, a.shippable, a.is_starred,
                a.address_type, a.address_name,
                jsonb_build_object(
                    'id', ma.id,
                    'name', ma.name,
                    'country', c.name,
                    'state', ms.abbreviation,
                    'timezone', jsonb_build_object(
                        'id', t.id,
                        'name', t.name,
                        'abbreviation', t.abbreviation
                    )
                ) AS metro_area,
                jsonb_build_object(
                    'id', s.id,
                    'name', s.name,
                    'abbreviation', s.abbreviation
                ) AS state
            FROM ams.ams_address a
            LEFT JOIN ams.metro_area ma ON a.metro_area_id = ma.id
            LEFT JOIN ams.country c ON ma.country_id = c.id
            LEFT JOIN ams.state ms ON ma.state_id = ms.id
            LEFT JOIN ams.timezone t ON ma.timezone = t.id::TEXT
            LEFT JOIN ams.state s ON a.state_id = s.id
            {address_type_filter}
            ORDER BY a.last_modified DESC
        """
        
        values = {}
        if address_type:
            values["address_type"] = address_type
            
        result = await get_pg_readonly_database().fetch_all(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_state_name_from_id(state_id: str) -> str | None:
    # Implement the logic to get the state name from the database.
    query = "SELECT name FROM ams.state where id = :state_id"
    value = {"state_id": state_id}
    result = await get_pg_database().fetch_val(query=query, values=value)
    return result


async def create_addresses(addresses: list[AddressRequestModel]):
    try:
        query = """
            INSERT INTO ams.ams_address (
            id, street_one, street_two, city, state_id, metro_area_id, postal_code, lat_long,
            address_type, address_name, shippable, notes)
            VALUES (
            :id, :street_one, :street_two, :city, :state_id, :metro_area_id, :postal_code, :lat_long,
            :address_type, :address_name, :shippable, :notes);
            """
        values = []
        for address in addresses:
            address_dict = address.model_dump()
            state_name = await get_state_name_from_id(address_dict["state_id"])
            if state_name:
                full_address = f"{address_dict['street_one']}, {address_dict['city']}, {state_name}"
            else:
                full_address = f"{address_dict['street_one']}, {address_dict['city']}"
            coordinates = await geocode_ams_address(full_address)
            if coordinates:
                address_dict["lat_long"] = coordinates
            else:
                address_dict["lat_long"] = None
            values.append({"id": str(uuid.uuid4()), **address_dict})

        await get_pg_database().execute_many(query=query, values=values)
        return {"inserted_count": len(addresses), "message": "Addresses created successfully."}
    except UniqueViolationError:  # Handle unique constraint violation
        raise HTTPException(status_code=400, detail="An address with these details already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_address(address_id: str, address: AddressRequestModel):
    try:
        query = """
            UPDATE ams.ams_address
            SET 
                street_one = :street_one,
                street_two = :street_two,
                city = :city,
                state_id = :state_id,
                metro_area_id = :metro_area_id,
                postal_code = :postal_code,
                lat_long = :lat_long,
                address_type = :address_type,
                address_name = :address_name,
                shippable = :shippable,
                notes = :notes,
                last_modified = CURRENT_TIMESTAMP
            WHERE id = :address_id
            RETURNING *;
            """
        address_dict = address.model_dump()
        state_name = await get_state_name_from_id(address_dict["state_id"])
        if state_name:
            full_address = f"{address_dict['street_one']}, {address_dict['city']}, {state_name}"
        else:
            full_address = f"{address_dict['street_one']}, {address_dict['city']}"
        coordinates = await geocode_ams_address(full_address)
        if coordinates:
            address_dict["lat_long"] = coordinates
        else:
            address_dict["lat_long"] = None

        values = {
            **address_dict,
            "address_id": address_id
        }
        result = await get_pg_database().fetch_one(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_address_star(address_id: str):
    try:
        query = f"""
        UPDATE ams.ams_address
        SET
            is_starred = NOT is_starred
        WHERE id = :address_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"address_id": address_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_searched_emails(
        page: int = 1,
        page_size: int = 10,
        sort_field: str = None,
        sort_order: str = 'desc',
        search_query: str = "",
        assigned_to_account: Literal["all", "yes", "no"] = "all",
        paid_account: Literal["all", "yes", "no"] = "all",
        status: Optional[Literal['AVAILABLE', 'IN USE', 'SUSPENDED', 'RETIRED']] = None
):
    offset = (page - 1) * page_size
    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    # Validate sort_field to prevent SQL injection and handle joins
    sortable_columns = [
        "email_address",
        "created",
        "person_name",
        "person_dob",
        "recovery_email_address",
        "is_starred",
        "last_modified",
        "status",
        "account_nickname"
    ]
    if sort_field not in sortable_columns:
        sort_field = "last_modified"  # Default to a safe column
    order_clause = f"{sort_field} {sort_order}"

    try:
        query = f"""
            WITH recovery_phone AS (
                SELECT
                    rp.id,
                    ph.number
                FROM ams.ams_recovery_phone rp
                JOIN ams.phone_number ph ON rp.phone_number_id = ph.id
            ),
            recovery_emails AS (
                SELECT
                    rem.ams_email_id,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'id', re.id,
                                'email_address', re.email_address
                            ) ORDER BY re.email_address ASC
                        ) FILTER (WHERE re.id IS NOT NULL),
                        '[]'::json
                    ) AS recovery_emails
                FROM ams.ams_email_recovery_email_mapping rem
                LEFT JOIN ams.ams_recovery_email re ON rem.ams_recovery_email_id = re.id
                GROUP BY rem.ams_email_id
            ),
            recovery_phones AS (
                SELECT
                    rpm.ams_email_id,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'id', rp.id,
                                'number', rp.number
                            ) ORDER BY rp.number ASC
                        ) FILTER (WHERE rp.id IS NOT NULL),
                        '[]'::json
                    ) AS recovery_phones
                FROM ams.ams_email_recovery_phone_mapping rpm
                LEFT JOIN recovery_phone rp ON rpm.ams_recovery_phone_id = rp.id
                GROUP BY rpm.ams_email_id
            ),
            filter_results AS (
                SELECT
                    e.id,
                    e.email_address,
                    e.status,
                    e.password,
                    e.created,
                    e.last_modified,
                    e.notes,
                    e.robot_check_phone,
                    e.paid_account,
                    e.backup_codes,
                    e.is_starred,
                    ac.id as account_id,
                    jsonb_build_object(
                        'id', p.id,
                        'first_name', p.first_name,
                        'last_name', p.last_name,
                        'date_of_birth', p.date_of_birth
                    ) AS person,
                    COALESCE(re.recovery_emails, '[]'::json) AS recovery_emails,
                    COALESCE(rp.recovery_phones, '[]'::json) AS recovery_phones,
                    jsonb_build_object(
                        'id', ph.id,
                        'number', ph.number
                    ) AS pva_phone,
                    ac.nickname as account_nickname,
                    p.first_name as person_name,
                    p.date_of_birth as person_dob
                FROM ams.ams_email e
                LEFT JOIN ams.ams_person p ON e.ams_person_id = p.id
                LEFT JOIN recovery_emails re ON e.id = re.ams_email_id
                LEFT JOIN recovery_phones rp ON e.id = rp.ams_email_id
                LEFT JOIN ams.phone_number ph ON e.pva_phone_id = ph.id
                LEFT JOIN ams.ams_account ac ON e.id = ac.ams_email_id
                WHERE 
				    (
                     COALESCE(e.email_address, '') || ' ' || 
				     COALESCE(p.first_name, '') || ' ' || 
				     COALESCE(p.last_name, '') || ' ' ||
                     COALESCE(ac.nickname, '')
				    ) ILIKE '%' || :search_query || '%'
					AND (:assigned_to_account = 'all' OR
						(:assigned_to_account = 'yes' AND ac.ams_email_id IS NOT NULL) OR
						(:assigned_to_account = 'no' AND ac.ams_email_id IS NULL))
					AND (:paid_account = 'all' OR
						(:paid_account = 'yes' AND e.paid_account IS TRUE) OR
						(:paid_account = 'no' AND e.paid_account IS FALSE))
				    AND (:status = '' OR e.status::text = :status)
            )
            SELECT *, (SELECT COUNT(*) FROM filter_results) AS total_count
            FROM filter_results
            ORDER BY {order_clause}
            LIMIT :page_size OFFSET :offset;
            """
        result = await get_pg_readonly_database().fetch_all(
            query=query, values={
                "search_query": search_query,
                "page_size": page_size,
                "offset": offset,
                "assigned_to_account": assigned_to_account,
                "paid_account": paid_account,
                "status": status if status else ""
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_email(email_id: str):
    try:
        query = """
            WITH recovery_phone AS (
                SELECT
                    rp.id,
                    ph.number
                FROM ams.ams_recovery_phone rp
                JOIN ams.phone_number ph ON rp.phone_number_id = ph.id
            ),
            recovery_emails AS (
                SELECT
                    rem.ams_email_id,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'id', re.id,
                                'email_address', re.email_address
                            ) ORDER BY re.email_address ASC
                        ) FILTER (WHERE re.id IS NOT NULL),
                        '[]'::json
                    ) AS recovery_emails
                FROM ams.ams_email_recovery_email_mapping rem
                LEFT JOIN ams.ams_recovery_email re ON rem.ams_recovery_email_id = re.id
                WHERE rem.ams_email_id = :email_id
                GROUP BY rem.ams_email_id
            ),
            recovery_phones AS (
                SELECT
                    rpm.ams_email_id,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'id', rp.id,
                                'number', rp.number
                            ) ORDER BY rp.number ASC
                        ) FILTER (WHERE rp.id IS NOT NULL),
                        '[]'::json
                    ) AS recovery_phones
                FROM ams.ams_email_recovery_phone_mapping rpm
                LEFT JOIN recovery_phone rp ON rpm.ams_recovery_phone_id = rp.id
                WHERE rpm.ams_email_id = :email_id
                GROUP BY rpm.ams_email_id
            ),
            forwarding_emails AS (
                SELECT
                    efm.ams_email_id,
                    COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'id', fe.id,
                                'email_address', fe.email_address,
                                'created_at', fe.created_at,
                                'last_modified', fe.last_modified
                            ) ORDER BY fe.email_address ASC
                        ) FILTER (WHERE fe.id IS NOT NULL),
                        '[]'::json
                    ) AS forwarding_emails
                FROM ams.ams_email_forwarding_map efm
                LEFT JOIN ams.ams_forwarding_email fe ON efm.ams_forwarding_email_id = fe.id
                WHERE efm.ams_email_id = :email_id
                GROUP BY efm.ams_email_id
            )
            SELECT
                e.id,
                e.email_address,
                e.status,
                e.password,
                e.created,
                e.last_modified,
                e.notes,
                e.robot_check_phone, 
                e.paid_account, 
                e.backup_codes,
                e.spam_filter_setup_completed, 
                e.catchall_forward_setup_completed,
                e.update_gmail_name, 
                e.gmail_filter_forwarding_setup,
                e.recovery_email_setup,
                e.recovery_phone_setup, 
                e.send_testing_email,
                e.is_starred,
                ac.id as account_id,
                jsonb_build_object(
                    'id', p.id,
                    'first_name', p.first_name,
                    'last_name', p.last_name,
                    'date_of_birth', p.date_of_birth
                ) AS person,
                COALESCE(re.recovery_emails, '[]'::json) AS recovery_emails,
                COALESCE(rp.recovery_phones, '[]'::json) AS recovery_phones,
                COALESCE(fe.forwarding_emails, '[]'::json) AS forwarding_emails,
                CASE 
                    WHEN ce.id IS NOT NULL THEN 
                        jsonb_build_object(
                            'id', ce.id,
                            'email_address', ce.email_address,
                            'created_at', ce.created_at,
                            'last_modified', ce.last_modified
                        )
                    ELSE NULL
                END AS catchall_email,
                jsonb_build_object(
                    'id', ph.id,
                    'number', ph.number
                ) AS pva_phone,
                ac.nickname as account_nickname,
                p.first_name as person_name,
                p.date_of_birth as person_dob
            FROM ams.ams_email e
            LEFT JOIN ams.ams_person p ON e.ams_person_id = p.id
            LEFT JOIN recovery_emails re ON e.id = re.ams_email_id
            LEFT JOIN recovery_phones rp ON e.id = rp.ams_email_id
            LEFT JOIN forwarding_emails fe ON e.id = fe.ams_email_id
            LEFT JOIN ams.ams_catchall_email ce ON e.catchall_email_id = ce.id
            LEFT JOIN ams.phone_number ph ON e.pva_phone_id = ph.id
            LEFT JOIN ams.ams_account ac ON e.id = ac.ams_email_id
            WHERE e.id = :email_id;
            """
        result = await get_pg_readonly_database().fetch_one(
            query=query, values={"email_id": email_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def insert_recovery_email_mappings(db, recovery_email_mappings):
    try:
        if not recovery_email_mappings:
            return
        query = """
            INSERT INTO ams.ams_email_recovery_email_mapping
                (id, ams_email_id, ams_recovery_email_id)
            VALUES (:id, :ams_email_id, :ams_recovery_email_id)
            ON CONFLICT (ams_email_id, ams_recovery_email_id) DO NOTHING;
        """
        await db.execute_many(query=query, values=recovery_email_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def insert_recovery_phone_mappings(db, recovery_phone_mappings):
    try:
        if not recovery_phone_mappings:
            return
        query = """
            INSERT INTO ams.ams_email_recovery_phone_mapping
                (id, ams_email_id, ams_recovery_phone_id)
            VALUES (:id, :ams_email_id, :ams_recovery_phone_id)
            ON CONFLICT (ams_email_id, ams_recovery_phone_id) DO NOTHING;
        """
        await db.execute_many(query=query, values=recovery_phone_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def insert_forwarding_email_mappings(db, forwarding_email_mappings):
    try:
        if not forwarding_email_mappings:
            return
        query = """
            INSERT INTO ams.ams_email_forwarding_map
                (id, ams_email_id, ams_forwarding_email_id)
            VALUES (:id, :ams_email_id, :ams_forwarding_email_id)
            ON CONFLICT (ams_email_id, ams_forwarding_email_id) DO NOTHING;
        """
        await db.execute_many(query=query, values=forwarding_email_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_emails(emails: list[EmailRequestModel]):
    """
    Bulk create multiple AMS emails in one operation.

    Builds a single multi-value INSERT statement for emails, executes it, then (if
    any recovery emails or phones were supplied) performs a second batched INSERT into ams_email_recovery_email_mapping and ams_email_recovery_phone_mapping.
    Recovery email and phone insertion is idempotent per (ams_email_id, ams_recovery_email_id) and (ams_email_id, ams_recovery_phone_id) due to ON CONFLICT DO NOTHING.

    Args:
        emails (List[EmailRequestModel]): Sequence of validated email request models.
    Returns:
        dict: Dictionary containing the number of emails inserted and a message.

    Raises:
        HTTPException:
            500 if any database error occurs during account or tag insertion.

    """
    try:
        db = get_pg_database()
        async with db.transaction():
            # Step 1: Generate UUIDs in Python
            email_ids = [str(uuid.uuid4()) for _ in emails]
            email_values = [
                {**email.model_dump(exclude={"recovery_email_ids", "recovery_phone_ids", "forwarding_email_ids"}), "id": email_id}
                for email, email_id in zip(emails, email_ids)
            ]
            print("Email values:", email_values)
            # Step 2: Bulk insert emails
            insert_query = """
                INSERT INTO ams.ams_email (
                    id, ams_person_id, created_by, email_address, password, status,
                    pva_phone_id, robot_check_phone, paid_account,
                    backup_codes, spam_filter_setup_completed, catchall_forward_setup_completed,
                    update_gmail_name, gmail_filter_forwarding_setup, recovery_email_setup, recovery_phone_setup,
                    send_testing_email, notes, catchall_email_id
                ) VALUES (
                    :id, :ams_person_id, :created_by, :email_address, 
                    :password, :status, :pva_phone_id, :robot_check_phone,
                    :paid_account, :backup_codes, :spam_filter_setup_completed, :catchall_forward_setup_completed, 
                    :update_gmail_name, :gmail_filter_forwarding_setup, :recovery_email_setup, :recovery_phone_setup,
                    :send_testing_email, :notes, :catchall_email_id
                );
            """
            await db.execute_many(query=insert_query, values=email_values)

            recovery_email_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_recovery_email_id": recovery_email_id
                }
                for email, email_id in zip(emails, email_ids)
                for recovery_email_id in (email.recovery_email_ids or [])
            ]
            await insert_recovery_email_mappings(db, recovery_email_mappings)
            
            recovery_phone_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_recovery_phone_id": recovery_phone_id
                }
                for email, email_id in zip(emails, email_ids)
                for recovery_phone_id in (email.recovery_phone_ids or [])
            ]
            await insert_recovery_phone_mappings(db, recovery_phone_mappings)

            forwarding_email_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_forwarding_email_id": forwarding_email_id
                }
                for email, email_id in zip(emails, email_ids)
                for forwarding_email_id in (email.forwarding_email_ids or [])
            ]
            await insert_forwarding_email_mappings(db, forwarding_email_mappings)

            return {
                "inserted_count": len(emails),
                "message": "Emails created and mappings inserted."
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_recovery_email_mappings(db, email_id: str, recovery_email_ids: list[str]):
    try:
        # Remove old mappings
        await db.execute(
            "DELETE FROM ams.ams_email_recovery_email_mapping WHERE ams_email_id = :email_id;",
            {"email_id": email_id}
        )
        # Insert new mappings
        if recovery_email_ids:
            recovery_email_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_recovery_email_id": recovery_email_id
                }
                for recovery_email_id in recovery_email_ids
            ]
            query = """
                INSERT INTO ams.ams_email_recovery_email_mapping
                    (id, ams_email_id, ams_recovery_email_id)
                VALUES (:id, :ams_email_id, :ams_recovery_email_id)
                ON CONFLICT (ams_email_id, ams_recovery_email_id) DO NOTHING;
            """
            await db.execute_many(query=query, values=recovery_email_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred updating recovery emails: {e}.")

async def update_recovery_phone_mappings(db, email_id: str, recovery_phone_ids: list[str]):
    try:
        # Remove old mappings
        await db.execute(
            "DELETE FROM ams.ams_email_recovery_phone_mapping WHERE ams_email_id = :email_id;",
            {"email_id": email_id}
        )
        # Insert new mappings
        if recovery_phone_ids:
            recovery_phone_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_recovery_phone_id": recovery_phone_id
                }
                for recovery_phone_id in recovery_phone_ids
            ]
            query = """
                INSERT INTO ams.ams_email_recovery_phone_mapping
                    (id, ams_email_id, ams_recovery_phone_id)
                VALUES (:id, :ams_email_id, :ams_recovery_phone_id)
                ON CONFLICT (ams_email_id, ams_recovery_phone_id) DO NOTHING;
            """
            await db.execute_many(query=query, values=recovery_phone_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred updating recovery phones: {e}.")


async def update_forwarding_email_mappings(db, email_id: str, forwarding_email_ids: list[str]):
    try:
        await db.execute(
            "DELETE FROM ams.ams_email_forwarding_map WHERE ams_email_id = :email_id;",
            {"email_id": email_id}
        )

        if forwarding_email_ids:
            forwarding_email_mappings = [
                {
                    "id": str(uuid.uuid4()),
                    "ams_email_id": email_id,
                    "ams_forwarding_email_id": forwarding_email_id
                }
                for forwarding_email_id in forwarding_email_ids
            ]
            query = """
                INSERT INTO ams.ams_email_forwarding_map
                    (id, ams_email_id, ams_forwarding_email_id)
                VALUES (:id, :ams_email_id, :ams_forwarding_email_id)
                ON CONFLICT (ams_email_id, ams_forwarding_email_id) DO NOTHING;
            """
            await db.execute_many(query=query, values=forwarding_email_mappings)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred updating forwarding emails: {e}.")


async def update_email(email_id: str, email: EmailRequestModel):
    db = get_pg_database()
    async with db.transaction():
        try:
            # Update main email record
            query = """
                UPDATE ams.ams_email
                SET 
                    ams_person_id = :ams_person_id,
                    created_by = :created_by,
                    email_address = :email_address,
                    password = :password,
                    status = :status,
                    pva_phone_id = :pva_phone_id,
                    robot_check_phone = :robot_check_phone,
                    paid_account = :paid_account,
                    backup_codes = :backup_codes,
                    spam_filter_setup_completed = :spam_filter_setup_completed,
                    catchall_forward_setup_completed = :catchall_forward_setup_completed,
                    update_gmail_name = :update_gmail_name,
                    gmail_filter_forwarding_setup = :gmail_filter_forwarding_setup,
                    recovery_email_setup = :recovery_email_setup,
                    recovery_phone_setup = :recovery_phone_setup,
                    send_testing_email = :send_testing_email,
                    notes = :notes,
                    catchall_email_id = :catchall_email_id,
                    last_modified = CURRENT_TIMESTAMP
                WHERE id = :email_id
                RETURNING *;
            """
            values = {
                **email.model_dump(exclude={"recovery_email_ids", "recovery_phone_ids", "forwarding_email_ids"}),
                "email_id": email_id
            }
            result = await db.fetch_one(query=query, values=values)

            await update_recovery_email_mappings(db, email_id, email.recovery_email_ids or [])
            await update_recovery_phone_mappings(db, email_id, email.recovery_phone_ids or [])
            await update_forwarding_email_mappings(db, email_id, email.forwarding_email_ids or [])

            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_emails_status(update_data):
    try:
        email_ids = update_data['email_ids']

        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(email_ids))])

        query = f"""
        UPDATE ams.ams_email
        SET
            status = :status,
            last_modified = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        RETURNING id;
        """

        # Create values dict with individual ID parameters
        values = {"status": update_data['status']}
        for i, pid in enumerate(email_ids):
            values[f"id{i}"] = pid

        result = await get_pg_database().fetch_all(
            query=query,
            values=values
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_email_star(email_id: str):
    try:
        query = f"""
        UPDATE ams.ams_email
        SET
            is_starred = NOT is_starred
        WHERE id = :email_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"email_id": email_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def bulk_update_recovery_email_mappings(db, email_ids: list[str], recovery_email_ids: list[str]):
    if not email_ids:
        return

    delete_query = f"DELETE FROM ams.ams_email_recovery_email_mapping WHERE ams_email_id = ANY(:email_ids)"
    delete_values = {"email_ids": email_ids}
    await db.execute(query=delete_query, values=delete_values)

    bulk_mappings = [
        {
            "id": str(uuid.uuid4()),
            "ams_email_id": email_id,
            "ams_recovery_email_id": recovery_email_id
        }
        for email_id in email_ids
        for recovery_email_id in recovery_email_ids
    ]
    if bulk_mappings:
        insert_query = """
            INSERT INTO ams.ams_email_recovery_email_mapping
            (id, ams_email_id, ams_recovery_email_id)
            VALUES (:id, :ams_email_id, :ams_recovery_email_id)
            ON CONFLICT (ams_email_id, ams_recovery_email_id) DO NOTHING;
        """
        await db.execute_many(query=insert_query, values=bulk_mappings)

async def bulk_update_recovery_phone_mappings(db, email_ids: list[str], recovery_phone_ids: list[str]):
    if not email_ids:
        return

    delete_query = f"DELETE FROM ams.ams_email_recovery_phone_mapping WHERE ams_email_id = ANY(:email_ids)"
    delete_values = {"email_ids": email_ids}
    await db.execute(query=delete_query, values=delete_values)

    bulk_mappings = [
        {
            "id": str(uuid.uuid4()),
            "ams_email_id": email_id,
            "ams_recovery_phone_id": recovery_phone_id
        }
        for email_id in email_ids
        for recovery_phone_id in recovery_phone_ids
    ]
    if bulk_mappings:
        insert_query = """
            INSERT INTO ams.ams_email_recovery_phone_mapping
            (id, ams_email_id, ams_recovery_phone_id)
            VALUES (:id, :ams_email_id, :ams_recovery_phone_id)
            ON CONFLICT (ams_email_id, ams_recovery_phone_id) DO NOTHING;
        """
        await db.execute_many(query=insert_query, values=bulk_mappings)


async def bulk_update_forwarding_email_mappings(db, email_ids: list[str], forwarding_email_ids: list[str]):
    if not email_ids:
        return
    
    delete_query = "DELETE FROM ams.ams_email_forwarding_map WHERE ams_email_id = ANY(:email_ids)"
    await db.execute(query=delete_query, values={"email_ids": email_ids})

    bulk_mappings = [
        {
            "id": str(uuid.uuid4()),
            "ams_email_id": email_id,
            "ams_forwarding_email_id": forwarding_email_id
        }
        for email_id in email_ids
        for forwarding_email_id in forwarding_email_ids
    ]
    if bulk_mappings:
        insert_query = """
            INSERT INTO ams.ams_email_forwarding_map
            (id, ams_email_id, ams_forwarding_email_id)
            VALUES (:id, :ams_email_id, :ams_forwarding_email_id)
            ON CONFLICT (ams_email_id, ams_forwarding_email_id) DO NOTHING;
        """
        await db.execute_many(query=insert_query, values=bulk_mappings)


async def update_email_common_fields(update_data: EmailCommonFieldsRequest):
    """
    Bulk update common fields for multiple emails, and update their recovery emails and phones in bulk.
    Expects:
        update_data = {
            "email_ids": [<str>, ...],
            "updated_fields": {
                "status": <str>,
                "paid_account": <bool>,
                "catchall_forward_setup_completed": <bool>,
                "spam_filter_setup_completed": <bool>,
                "update_gmail_name": <bool>,
                "gmail_filter_forwarding_setup": <bool>,
                "recovery_email_setup": <bool>,
                "recovery_phone_setup": <bool>,
                "send_testing_email": <bool>,
                "recovery_email_ids_by_email_id": [<recovery_email_id>, ...],
                "recovery_phone_ids_by_email_id": [<recovery_phone_id>, ...]
            }
        }
    """
    try:
        db = get_pg_database()
        email_ids = update_data.email_ids
        updated_fields = update_data.updated_fields
        recovery_email_ids = updated_fields.recovery_email_ids or []
        recovery_phone_ids = updated_fields.recovery_phone_ids or []
        forwarding_email_ids = updated_fields.forwarding_email_ids or []

        async with db.transaction():
            # Bulk update common fields for all emails
            placeholders = ', '.join([f':id{i}' for i in range(len(email_ids))])
            query = f"""
            UPDATE ams.ams_email
            SET
                status = :status,
                paid_account = :paid_account,
                catchall_forward_setup_completed = :catchall_forward_setup_completed,
                spam_filter_setup_completed = :spam_filter_setup_completed,
                update_gmail_name = :update_gmail_name,
                gmail_filter_forwarding_setup = :gmail_filter_forwarding_setup,
                recovery_email_setup = :recovery_email_setup,
                recovery_phone_setup = :recovery_phone_setup,
                send_testing_email = :send_testing_email,
                catchall_email_id = :catchall_email_id,
                last_modified = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders})
            RETURNING id;
            """
            values = {
                "status": updated_fields.status,
                "paid_account": updated_fields.paid_account,
                "catchall_forward_setup_completed": updated_fields.catchall_forward_setup_completed,
                "spam_filter_setup_completed": updated_fields.spam_filter_setup_completed,
                "update_gmail_name": updated_fields.update_gmail_name,
                "gmail_filter_forwarding_setup": updated_fields.gmail_filter_forwarding_setup,
                "recovery_email_setup": updated_fields.recovery_email_setup,
                "recovery_phone_setup": updated_fields.recovery_phone_setup,
                "send_testing_email": updated_fields.send_testing_email,
                "catchall_email_id": updated_fields.catchall_email_id,
            }
            for i, pid in enumerate(email_ids):
                values[f"id{i}"] = pid

            result = await db.fetch_all(query=query, values=values)

            await bulk_update_recovery_email_mappings(db, email_ids, recovery_email_ids)
            await bulk_update_recovery_phone_mappings(db, email_ids, recovery_phone_ids)
            await bulk_update_forwarding_email_mappings(db, email_ids, forwarding_email_ids)

            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_searched_accounts(
        page: int = 1,
        page_size: int = 10,
        sort_field: str = None,
        sort_order: str = 'desc',
        search_query: str = "",
        metro_area_ids: list[str] = None,
        company_ids: list[str] = None,
        created_at: str = None,  # Input will be a string "MM-DD-YYYY"
        status_code: Optional[List[str]] = None,
        address_search_query: str = "",
        incomplete_steps: Optional[list[str]] = None):
    offset = (page - 1) * page_size
    # Normalize optional text filters so the wildcard search behaves as expected.
    search_query = search_query or ""
    address_search_query = address_search_query or ""
    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    # Validate sort_field to prevent SQL injection and handle joins
    sortable_columns = [
        "metro_area_name",
        "nickname",
        "person_first_name",
        "address_street_one",
        "created_at",
        "is_starred",
        "last_modified",
    ]
    if sort_field not in sortable_columns:
        sort_field = "last_modified"  # Default to a safe column
    order_clause = f"{sort_field} {sort_order}"

    # Handle created_at filter
    created_at_condition = "TRUE"
    parsed_created_at_date = None
    if created_at:
        try:
            # Parse the date string "MM-DD-YYYY" into a datetime object
            parsed_created_at_date = datetime.strptime(created_at, "%m/%d/%Y").date()
            # The condition will filter for accounts created on or after this date, up to the end of the day.
            created_at_condition = "a.created_at >= :start_of_day_created_at AND a.created_at < (:start_of_day_created_at + INTERVAL '1 DAY')"
        except ValueError:
            # Handle cases where the date format is incorrect
            raise HTTPException(status_code=400, detail="Invalid created_at date format. Expected MM-DD-YYYY.")

    try:
        values = {}
        # Build the metro area filter condition
        metro_filter = ""
        if metro_area_ids and len(metro_area_ids) > 0:
            # Create placeholders for each metro area ID
            placeholders = ",".join([f"'{id}'" for id in metro_area_ids])
            metro_filter = f"AND ma.id IN ({placeholders})"

        status_filter = ""
        if status_code and len(status_code) > 0:
            status_filter = " AND a.status_code = ANY(:status_code)"
            values.update({"status_code": status_code})

        company_filter = ""
        if company_ids and len(company_ids) > 0:
            company_filter = "AND a.company_id = ANY(:company_ids)"
            values.update({"company_ids": company_ids})           

        # Check if incomplete_steps is None or empty
        if incomplete_steps is None or len(incomplete_steps) == 0:
            incomplete_steps_condition = "TRUE"
            values.update({
                "search_query": search_query,
                "address_search_query": address_search_query,
                "page_size": page_size,
                "offset": offset,
            })
        else:
            incomplete_steps_condition = """
                NOT EXISTS (
                    SELECT 1
                    FROM ams.account_completed_steps acs_filter
                    WHERE acs_filter.account_id::UUID = a.id
                      AND acs_filter.step_id = ANY(CAST(:incomplete_steps AS UUID[]))
                )
            """
            values.update({
                "search_query": search_query,
                "page_size": page_size,
                "offset": offset,
                "incomplete_steps": incomplete_steps,
            })
        # Add parsed_created_at_date to values if it exists
        if parsed_created_at_date:
            values["start_of_day_created_at"] = parsed_created_at_date

        query = f"""
            WITH filter_results AS (
                SELECT 
                    a.id,
                    aas.label as status,
                    (
                        SELECT COUNT(*)
                        FROM ams.ams_credit_card cc
                        WHERE cc.ams_account_id = a.id
                    ) AS cc_count,
                    a.nickname, a.status_code, a.created_at, a.last_modified, a.is_starred, a.notes, a.ams_proxy_id, a.multilogin_id,
                    jsonb_build_object(
                        'id', ma.id,
                        'name', ma.name,
                        'country', c.name,
                        'state', ms.abbreviation,
                        'timezone', jsonb_build_object(
                            'id', t.id,
                            'name', t.name,
                            'abbreviation', t.abbreviation
                        )
                    ) AS metro_area,
                    CASE 
                        WHEN p.id IS NOT NULL THEN jsonb_build_object(
                            'id', p.id,
                            'first_name', p.first_name,
                            'last_name', p.last_name,
                            'date_of_birth', p.date_of_birth,
                            'status', p.status
                        )
                        ELSE NULL
                    END AS person,
                    jsonb_build_object(
                        'id', d.id,
                        'street_one', d.street_one,
                        'street_two', d.street_two,
                        'city', d.city,
                        'state', jsonb_build_object(
                            'id', ds.id,
                            'name', ds.name,
                            'abbreviation', ds.abbreviation
                        ),
                        'postal_code', d.postal_code,
                        'metro_area', jsonb_build_object(
                            'name', ma.name,
                            'state', ms.abbreviation
                        )
                    ) AS address,
                    CASE 
                        WHEN e.id IS NOT NULL THEN jsonb_build_object(
                            'id', e.id,
                            'email_address', e.email_address,
                            'status', e.status
                        )
                        ELSE NULL
                    END AS email,
                    CASE 
                        WHEN ph.id IS NOT NULL THEN jsonb_build_object(
                            'id', ph.id,
                            'number', ph.number,
                            'status', ph.status
                        )
                        ELSE NULL
                    END AS phone,
                    CASE 
                        WHEN com.id IS NOT NULL THEN jsonb_build_object(
                            'id', com.id,
                            'name', com.name
                        )
                        ELSE NULL
                    END AS company,
                    (
                        SELECT COUNT(*)
                        FROM ams.account_completed_steps acs
                        WHERE acs.account_id::UUID = a.id
                    ) AS completed_steps,
                    (
                        SELECT COUNT(*) FROM ams.steps
                    ) AS total_steps,
                    ma.name AS metro_area_name,  -- Include this for sorting
                    p.first_name AS person_first_name,  -- Include this for sorting
                    d.street_one AS address_street_one  -- Include this for sorting
                FROM ams.ams_account AS a
                JOIN ams.ams_address AS d ON a.ams_address_id = d.id
                LEFT JOIN ams.state AS ds ON d.state_id = ds.id
                LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
                LEFT JOIN ams.country AS c ON ma.country_id = c.id
                LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
                LEFT JOIN ams.timezone AS t ON ma.timezone = t.id::TEXT
                LEFT JOIN ams.ams_person p ON a.ams_person_id = p.id
                LEFT JOIN ams.ams_email e ON a.ams_email_id = e.id
                LEFT JOIN ams.phone_number ph ON a.phone_number_id = ph.id
                LEFT JOIN ams.company com ON a.company_id = com.id::TEXT
                LEFT JOIN ams.ams_account_status aas ON a.status_code = aas.code
                WHERE 
                (
						REGEXP_REPLACE(
							COALESCE(p.first_name, '') || ' ' ||
							COALESCE(p.last_name, '') || ' ' ||
							COALESCE(a.nickname, '') || ' ' ||
							COALESCE(a.id::text, '') || ' ' ||
							COALESCE(e.email_address, ''),
							'\s+',
							' ',
							'g'
						) ILIKE '%' || :search_query || '%'
					 )
                AND (
                    COALESCE(ds.name, '') || ' ' ||
                    COALESCE(ds.abbreviation, '') || ' ' ||
                    COALESCE(d.street_one, '') || ' ' ||
                    COALESCE(d.street_two, '') || ' ' ||
                    COALESCE(d.city, '')
                ) ILIKE '%' || :address_search_query || '%'
                AND ({incomplete_steps_condition})
                AND ({created_at_condition})
                {metro_filter}
                {status_filter}
                {company_filter}
            )
            SELECT *, (SELECT COUNT(*) FROM filter_results) AS total_count
            FROM filter_results
            ORDER BY {order_clause}
            LIMIT :page_size OFFSET :offset;
            """

        result = await get_pg_readonly_database().fetch_all(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


def convert_accounts_to_csv(accounts_data: List[Dict[str, Any]]) -> str:
    """
    Convert accounts data to CSV format matching the frontend table display.
    This function takes the output from get_searched_accounts and converts it to CSV.
    """
    if not accounts_data:
        return ""

    def format_completion(completed_steps, total_steps):
        if not total_steps or total_steps < 1:
            return "No Steps"
        if completed_steps is None or completed_steps > total_steps:
            return "ERR" if completed_steps and completed_steps > total_steps else f"0/{total_steps}"
        return f"{completed_steps}/{total_steps}"

    def format_date(date):
        if not date:
            return ""
        return date.strftime('%m/%d/%Y').lstrip('0').replace('/0', '/')

    def safe_json_loads(json_str):
        """Safely parse JSON string, return empty dict if None or invalid JSON"""
        if json_str is None:
            return {}
        try:
            parsed = json.loads(json_str)
            # If parsed is None or all values are None, return empty dict
            if parsed is None or (isinstance(parsed, dict) and all(v is None for v in parsed.values())):
                return {}
            return parsed
        except (json.JSONDecodeError, TypeError):
            return {}

    # Define CSV column headers that match EXACTLY the frontend table column titles
    fieldnames = [
        'Metro Area',
        'Nickname',
        'Created',
        'Credit Cards',
        'Person Name',
        'Address',
        'Phone',
        'Email',
        'Completion',
        'Company',
        'Status'
    ]

    # Convert accounts to CSV format
    csv_rows = []
    for account in accounts_data:
        metro_area = safe_json_loads(account.get('metro_area'))
        person = safe_json_loads(account.get('person'))
        address = safe_json_loads(account.get('address'))
        phone = safe_json_loads(account.get('phone'))
        email = safe_json_loads(account.get('email'))
        company = safe_json_loads(account.get('company'))

        csv_row = {
            'Metro Area': f"{metro_area.get('name', '')}, {metro_area.get('state', '')} {metro_area.get('country', '')} ({metro_area.get('timezone', {}).get('abbreviation', '')})",

            'Nickname': account.get('nickname', ''),

            'Created': format_date(account.get('created_at')),

            'Credit Cards': 0,

            'Person Name': f"{person.get('first_name', '')} {person.get('last_name', '')}".strip(),

            'Address': ', '.join(filter(None, [
                address.get('street_one', ''),
                address.get('street_two', ''),
                f"{address.get('city', '')}, {address.get('state', {}).get('abbreviation', '')} {address.get('postal_code', '')}".strip()
            ])),

            'Phone': format_phone_number(phone.get('number')) if phone and phone.get('number') is not None else "No phone assigned",

            'Email': email.get('email_address', ''),

            'Completion': format_completion(account.get('completed_steps'), account.get('total_steps')),

            'Company': company.get('name', ''),

            'Status': account.get('status', '')
        }
        csv_rows.append(csv_row)

    # Generate CSV string
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_rows)

    return output.getvalue()


async def get_searched_accounts_for_csv_export(
        sort_field: Optional[str] = None,
        sort_order: str = 'desc',
        search_query: str = "",
        metro_area_ids: Optional[list[str]] = None,
        company_ids: Optional[list[str]] = None,
        created_at: Optional[str] = None,
        status_code: Optional[List[str]] = None,
        incomplete_steps: Optional[list[str]] = None,
        address_search_query: Optional[str] = None,
        limit: int = 20000):
    """
    Get filtered accounts for CSV export using the existing get_searched_accounts logic.
    Returns CSV-formatted data ready for streaming.
    """
    try:
        # Use the existing get_searched_accounts function but without pagination
        # We'll call it with a large page size to get all results up to the limit
        accounts = await get_searched_accounts(
            page=1,
            page_size=limit,
            sort_field=sort_field,
            sort_order=sort_order,
            search_query=search_query,
            metro_area_ids=metro_area_ids,
            company_ids=company_ids,
            created_at=created_at,
            status_code=status_code,
            incomplete_steps=incomplete_steps,
            address_search_query=address_search_query
        )
        # Convert to list of dictionaries
        accounts_data = [dict(account) for account in accounts]

        # Convert to CSV format
        return convert_accounts_to_csv(accounts_data)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_account(account_id: str):
    try:
        query = """
            SELECT a.*, d.metro_area_id FROM ams.ams_account a 
            JOIN ams.ams_address d ON a.ams_address_id = d.id
            WHERE a.id = :account_id;
        """
        result = await get_pg_readonly_database().fetch_one(query=query, values={"account_id": account_id})
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_accounts_data_by_ids(account_ids: list[str]):
    try:
        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(account_ids))])

        query = f"""
            SELECT 
                a.id, a.nickname, a.status_code, a.created_at, a.ams_proxy_id, a.multilogin_id, a.notes,
                jsonb_build_object(
                    'id', d.id,
                    'street_one', d.street_one,
                    'street_two', d.street_two,
                    'city', d.city,
                    'state', jsonb_build_object(
                        'id', addr_state.id,
                        'name', addr_state.name,
                        'abbreviation', addr_state.abbreviation
                    ),
                    'postal_code', d.postal_code,
                    'metro_area', jsonb_build_object(
                        'id', ma.id,
                        'name', ma.name,
                        'state', jsonb_build_object(
                            'id', ma_state.id,
                            'name', ma_state.name,
                            'abbreviation', ma_state.abbreviation
                        ),
                        'country', c.name
                    )
                ) AS address,
                jsonb_build_object(
                    'id', p.id,
                    'first_name', p.first_name,
                    'last_name', p.last_name,
                    'full_name', p.full_name
                ) AS person,
                jsonb_build_object(
                    'id', e.id,
                    'email_address', e.email_address,
                    'password', e.password,
                    'recovery_email', re.email_address
                ) AS email,
                ph.number AS phone_number,
                (
                    SELECT COUNT(*)
                    FROM ams.account_completed_steps acs
                    WHERE acs.account_id::UUID = a.id
                ) AS completed_steps,
                (
                    SELECT COUNT(*)
                    FROM ams.steps s
                ) AS total_steps,
                jsonb_build_object(
                    'id', com.id,
                    'name', com.name
                ) AS company,
                jsonb_build_object(
                    'id', prox.id,
                    'proxy', prox.proxy,
                    'name', proxp.name
                ) AS proxy_data
            FROM ams.ams_account AS a
            JOIN ams.ams_address AS d ON a.ams_address_id = d.id
            LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
            LEFT JOIN ams.country AS c ON ma.country_id = c.id
            LEFT JOIN ams.state AS ma_state ON ma.state_id = ma_state.id
            LEFT JOIN ams.state AS addr_state ON d.state_id = addr_state.id
            LEFT JOIN ams.ams_person p ON a.ams_person_id = p.id
            LEFT JOIN ams.ams_email e ON a.ams_email_id = e.id
            LEFT JOIN ams.phone_number ph ON a.phone_number_id = ph.id
            LEFT JOIN ams.company com ON a.company_id = com.id::TEXT
            LEFT JOIN ams.ams_proxy prox ON a.ams_proxy_id = prox.id
            LEFT JOIN ams.ams_proxy_provider proxp ON prox.provider_id = proxp.id
            LEFT JOIN ams.ams_email_recovery_email_mapping aerem ON e.id = aerem.ams_email_id
            LEFT JOIN ams.ams_recovery_email re ON aerem.ams_recovery_email_id = re.id
            WHERE a.id IN ({placeholders});
            """
        values = {}
        for i, pid in enumerate(account_ids):
            values[f"id{i}"] = pid
        result = await get_pg_readonly_database().fetch_all(query=query, values=values)
        return result
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_account(account_id: str, updated_data: dict[str, str | None]):
    try:
        async with get_pg_database().transaction():
            current_proxy_query = """
                SELECT ams_proxy_id FROM ams.ams_account WHERE id = :account_id
            """
            current_proxy_result = await get_pg_database().fetch_one(
                query=current_proxy_query,
                values={"account_id": account_id}
            )

            current_proxy_id = current_proxy_result["ams_proxy_id"] if current_proxy_result else None
            new_proxy_id = updated_data.get("ams_proxy_id")

            current_email_query = """
                SELECT ams_email_id FROM ams.ams_account WHERE id = :account_id
            """
            current_email_result = await get_pg_database().fetch_one(
                query=current_email_query,
                values={"account_id": account_id}
            )

            current_email_id = current_email_result["ams_email_id"] if current_email_result else None
            new_email_id = updated_data.get("ams_email_id")

            query = """
                UPDATE ams.ams_account a
                SET
                    nickname = :nickname,
                    ams_person_id = :ams_person_id,
                    ams_email_id = :ams_email_id,
                    ams_address_id = :ams_address_id,
                    ams_proxy_id = :ams_proxy_id,
                    phone_number_id = :phone_number_id,
                    company_id = :company_id,
                    status_code = :status_code,
                    status_id = s.id,
                    notes = :notes,
                    last_modified = CURRENT_TIMESTAMP
                FROM ams.ams_account_status s
                WHERE a.id = :account_id
                  AND s.code = :status_code
                RETURNING a.*;
            """
            result = await get_pg_database().fetch_one(
                query=query,
                values={
                    "account_id": account_id,
                    **updated_data
                }
            )

            if current_proxy_id != new_proxy_id:
                if current_proxy_id:
                    await update_proxies_status({
                        'proxy_ids': [current_proxy_id],
                        'status': 'AVAILABLE'
                    })

                if new_proxy_id:
                    await update_proxies_status({
                        'proxy_ids': [new_proxy_id],
                        'status': 'IN_USE'
                    })

            if current_email_id != new_email_id:
                if current_email_id:
                    await update_emails_status({
                        'email_ids': [current_email_id],
                        'status': 'RETIRED'
                    })

                if new_email_id:
                    await update_emails_status({
                        'email_ids': [new_email_id],
                        'status': 'IN USE'
                    })

            return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")



async def update_account_star(account_id: str):
    try:
        query = """
        UPDATE ams.ams_account
        SET
            is_starred = NOT is_starred
        WHERE id = :account_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"account_id": account_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_accounts_nickname(account_id: str, update_data: dict[Literal['nickname'], str]):
    try:
        query = """
            UPDATE ams.ams_account
            SET 
                nickname = :nickname,
                last_modified = CURRENT_TIMESTAMP
            WHERE id = :account_id
            RETURNING *;
            """
        result = await get_pg_database().fetch_one(
            query=query, values={
                "account_id": account_id,
                "nickname": update_data['nickname'],
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_accounts_steps(steps: list[AcctStepRequestModel]):
    try:
        query = """
            UPDATE ams.account_completed_steps
            SET 
                value = :value,
                last_modified = CURRENT_TIMESTAMP
            WHERE account_id = :account_id
            AND step = :step;
            """
        values = [
            {
                "account_id": step.account_id,
                "step": step.step,
                "value": step.value
            }
            for step in steps
        ]
        await get_pg_database().execute_many(query=query, values=values)
        return {"updated_count": len(steps), "message": "Steps updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_accounts_notes(account_id: str, update_data: dict[Literal['notes'], str]):
    try:
        query = """
            UPDATE ams.ams_account
            SET 
                notes = :notes,
                last_modified = CURRENT_TIMESTAMP
            WHERE id = :account_id
            RETURNING *;
            """
        result = await get_pg_database().fetch_one(
            query=query, values={
                "account_id": account_id,
                "notes": update_data['notes'],
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_account_tags():
    db = get_pg_database()
    try:
        query = """
            SELECT id, name, created_at, last_modified
            FROM ams.ams_account_tag
            ORDER BY name ASC
        """
        rows = await db.fetch_all(query=query)
        return [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tags: {e}")


async def get_account_status_options():
    try:
        query = """
            SELECT code, description, label
            FROM ams.ams_account_status
            """
        rows = await get_pg_readonly_database().fetch_all(query=query)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch account status options: {e}")


async def get_proxy_status_options():
    try:
        query = """
            SELECT code, description, label
            FROM ams.ams_proxy_status
            """
        rows = await get_pg_readonly_database().fetch_all(query=query)
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch proxy status options: {e}")


async def get_account_tags_for_account(account_ids: list[str]):
    db = get_pg_readonly_database()
    try:
        query = """
            SELECT
                atm.account_id,
                t.id AS tag_id,
                t.name AS tag_name
            FROM ams.account_tag_mapping atm
            INNER JOIN ams.ams_account_tag t
                ON t.id = atm.tag_id
            WHERE atm.account_id = ANY(CAST(:account_ids AS uuid[]))
            ORDER BY atm.account_id, t.name ASC;
        """
        rows = await db.fetch_all(query=query, values={"account_ids": account_ids})
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            account_id = str(row["account_id"])
            grouped.setdefault(account_id, []).append(
                {
                    "id": str(row["tag_id"]),
                    "name": row["tag_name"],
                }
            )
        return grouped
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch account tags: {e}")


async def create_account_tag(name: str):
    db = get_pg_database()
    try:
        query = """
            INSERT INTO ams.ams_account_tag (id, name, created_at, last_modified)
            VALUES (:id, :name, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id, name, created_at, last_modified
        """
        values = {
            "id": str(uuid.uuid4()),
            "name": name.strip(),
        }

        row = await db.fetch_one(query=query, values=values)
        return row
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create tag: {e}")


async def update_accounts_tags(account_id: str, update_data: dict[Literal['tags'], list[str]]):
    try:
        db = get_pg_database()
        tags = update_data.get("tags", [])

        async with db.transaction():
            delete_query = """
                DELETE FROM ams.account_tag_mapping
                WHERE account_id = :account_id
            """
            await db.execute(query=delete_query, values={"account_id": account_id})

            inserted = []
            if tags:
                values = []
                for tag_id in tags:
                    values.append((
                        str(uuid.uuid4()),
                        account_id,
                        tag_id
                    ))

                insert_query = """
                    INSERT INTO ams.account_tag_mapping (id, account_id, tag_id, created_at, last_modified)
                    VALUES {}
                    ON CONFLICT (account_id, tag_id) DO NOTHING
                    RETURNING id, account_id, tag_id, created_at, last_modified
                """.format(
                    ",".join([
                        f"(:id{i}, :account_id{i}, :tag_id{i}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                        for i in range(len(values))
                    ])
                )

                flat_values = {}
                for i, (id_, acc, tag) in enumerate(values):
                    flat_values[f"id{i}"] = id_
                    flat_values[f"account_id{i}"] = acc
                    flat_values[f"tag_id{i}"] = tag

                inserted = await db.fetch_all(query=insert_query, values=flat_values)

        return inserted
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_accounts_bulk(update_data: dict):
    """
    Bulk update multiple accounts' status and/or tags.
    
    Args:
        update_data: Dictionary containing:
            - account_ids: List of account IDs to update
            - status_code: Optional status code to update (if provided)
            - tags: Optional list of tag IDs to add (if provided)
    
    Returns:
        dict: Success message and count of updated accounts
    """
    try:
        db = get_pg_database()
        account_ids = update_data.get("account_ids", [])
        status_code = update_data.get("status_code")
        tags = update_data.get("tags", [])
        
        if not account_ids:
            raise HTTPException(status_code=400, detail="account_ids is required")
        
        async with db.transaction():
            # Update status if provided
            placeholders = ', '.join([f':id{i}' for i in range(len(account_ids))])
            if status_code is not None:
                status_update_query = f"""
                UPDATE ams.ams_account a
                SET
                    status_code = :status_code,
                    status_id = s.id,
                    last_modified = CURRENT_TIMESTAMP
                FROM ams.ams_account_status s
                WHERE s.code = :status_code
                  AND a.id IN ({placeholders})
                RETURNING a.id;
                """

                # Create values dict with individual ID parameters
                values = {"status_code": update_data['status_code']}
                for i, pid in enumerate(account_ids):
                    values[f"id{i}"] = pid

                await db.execute(
                    query=status_update_query,
                    values=values
                )

            # Add tags if provided (only add tags that don't already exist)
            if tags:
                # Build list of all tag mappings for all accounts
                tag_mappings = []
                for account_id in account_ids:
                    for tag_id in tags:
                        tag_mappings.append({
                            "id": str(uuid.uuid4()),
                            "account_id": account_id,
                            "tag_id": tag_id
                        })
                
                # Bulk insert all tag mappings in a single query
                if tag_mappings:
                    values_placeholders = ",".join([
                        f"(:id{i}, :account_id{i}, :tag_id{i}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                        for i in range(len(tag_mappings))
                    ])
                    
                    insert_tags_query = f"""
                        INSERT INTO ams.account_tag_mapping (id, account_id, tag_id, created_at, last_modified)
                        VALUES {values_placeholders}
                        ON CONFLICT (account_id, tag_id) DO NOTHING
                    """
                    
                    # Flatten the values for the query
                    flat_values = {}
                    for i, mapping in enumerate(tag_mappings):
                        flat_values[f"id{i}"] = mapping["id"]
                        flat_values[f"account_id{i}"] = mapping["account_id"]
                        flat_values[f"tag_id{i}"] = mapping["tag_id"]
                    
                    await db.execute(query=insert_tags_query, values=flat_values)
        
        return {
            "success": True,
            "message": f"Successfully updated {len(account_ids)} account(s)",
            "updated_count": len(account_ids)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_account_items_by_metro_id(
    quantity: int = 1,
    metro_area_id: str = '',
    account_id: str = None,
    has_dob: bool = False,
    has_ssn: bool = False
):
    try:
        persons_query_with_account = ""
        person_values = {"metro_area_id": metro_area_id}
        if account_id:
            person_values["account_id"] = account_id
            # write a condition to include the current item when there's an account_id
            persons_query_with_account = """
            OR EXISTS (
                    SELECT 1
                    FROM ams.ams_account a_inner
                    WHERE id = :account_id
                    AND a_inner.ams_person_id = p.id
                )
            """

        # Build filtering conditions for DOB and SSN
        dob_ssn_conditions = ""
        if has_dob and has_ssn:
            dob_ssn_conditions = "AND p.date_of_birth IS NOT NULL AND p.last_4_ssn IS NOT NULL"
        elif has_dob:
            dob_ssn_conditions = "AND p.date_of_birth IS NOT NULL"
        elif has_ssn:
            dob_ssn_conditions = "AND p.last_4_ssn IS NOT NULL"

        persons_query = f"""
            SELECT
                p.id,
                p.first_name,
                p.last_name,
                p.date_of_birth,
                p.last_4_ssn
            FROM ams.ams_person p
            WHERE p.status = 'Active'
                AND NOT EXISTS (
                    SELECT 1 
                    FROM ams.ams_account a_inner
                    JOIN ams.ams_address addr ON a_inner.ams_address_id = addr.id
                    WHERE a_inner.ams_person_id = p.id 
                        AND addr.metro_area_id = :metro_area_id
                ) 
                {dob_ssn_conditions}
                {persons_query_with_account}
            ORDER BY p.first_name, p.last_name, p.date_of_birth, p.id;
            """
        persons_result = await get_pg_readonly_database().fetch_all(
            query=persons_query, values=person_values
        )
        available_persons = [dict(item) for item in persons_result]

        emails_query_with_account = ""
        email_values = {}
        if account_id:
            email_values["account_id"] = account_id
            # write a condition to include the current item when there's an account_id
            emails_query_with_account = """
            OR (a.id = :account_id AND a.ams_email_id = e.id)
            """

        emails_query = f"""
            SELECT
                e.id,
                e.email_address,
                e.ams_person_id
            FROM ams.ams_email e
            LEFT JOIN ams.ams_account a ON e.id = a.ams_email_id
            WHERE UPPER(e.status::varchar) = 'AVAILABLE'
                AND a.id IS NULL  -- Find emails not used in any account
                {emails_query_with_account}
            ORDER BY RANDOM();
        """

        emails_result = await get_pg_readonly_database().fetch_all(
            query=emails_query, values=email_values
        )
        available_emails = [dict(item) for item in emails_result]

        addresses_query_with_account = ""
        addresses_values = {"metro_area_id": metro_area_id}
        if account_id:
            addresses_values["account_id"] = account_id
            # write a condition to include the current item when there's an account_id
            addresses_query_with_account = """
            OR a.id = :account_id
            """

        addresses_query = f"""
            SELECT
                d.id, d.street_one, d.street_two, d.postal_code,
                jsonb_build_object(
                    'name', ma.name,
                    'state', ms.abbreviation
                ) AS metro_area
            FROM ams.ams_address d
            LEFT JOIN ams.ams_account a ON d.id = a.ams_address_id
            LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
            LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
            WHERE d.metro_area_id = :metro_area_id  -- Get an address in the same metro area
            AND (a.id IS NULL  -- Ensure the address is not already used in an account
            {addresses_query_with_account})
            ORDER BY RANDOM()  -- Pick random available addresses
            """

        addresses_result = await get_pg_readonly_database().fetch_all(
            query=addresses_query, values=addresses_values
        )
        available_addresses = [dict(item) for item in addresses_result]

        phones_query_with_account = ""
        phones_values = {}
        if account_id:
            phones_values["account_id"] = account_id
            # write a condition to include the current item when there's an account_id
            phones_query_with_account = """
            OR (a.id = :account_id AND a.phone_number_id = ph.id)
            """

        phones_query = f"""
            SELECT
                ph.id,
                ph.number
            FROM ams.phone_number ph
            LEFT JOIN ams.ams_account a ON ph.id = a.phone_number_id
            WHERE a.id IS NULL  -- Find phones not used in any account
            AND ph.status = 'Active'  -- Ensure the phone is active
            {phones_query_with_account}
            ORDER BY RANDOM()  -- Pick random available phones
            """

        phones_result = await get_pg_readonly_database().fetch_all(
            query=phones_query, values=phones_values
        )
        available_phones = [dict(item) for item in phones_result]

        proxies_query_with_account = ""
        proxies_values = {}
        if account_id:
            proxies_values["account_id"] = account_id
            proxies_query_with_account = """
                OR (a.id = :account_id AND a.ams_proxy_id = p.id)
                """
        
        proxies_query_with_metro_area = ""
        if metro_area_id:
            proxies_query_with_metro_area = ""
        else:
            proxies_query_with_metro_area = "AND (p.proxy_metro IS NULL)"

        proxies_query = f"""
            SELECT ap.id,
                ap.proxy,
                ap.proxy_metro,
                NULL AS proxy_metro_name,
                NULL AS proxy_lat,
                NULL AS proxy_lon
            FROM ams.ams_account ac
            JOIN ams.ams_proxy ap on ap.id = ac.ams_proxy_id
            WHERE ac.id = :account_id

            UNION

            SELECT
                p.id,
                p.proxy,
                p.proxy_metro,
                ma_proxy.name || ', ' || s.abbreviation || ' ' || c.name AS proxy_metro_name,
                ma_proxy.latitude  AS proxy_lat,
                ma_proxy.longitude AS proxy_lon
            FROM ams.ams_proxy p
            LEFT JOIN ams.ams_account a ON p.id = a.ams_proxy_id
            LEFT JOIN ams.metro_area ma_proxy ON p.proxy_metro = ma_proxy.id
            LEFT JOIN ams.state s ON s.id = ma_proxy.state_id
            LEFT JOIN ams.country c ON c.id = ma_proxy.country_id
            WHERE p.status_code = 'AVAILABLE'
                AND (
                    a.id IS NULL
                    {proxies_query_with_account}
                )
                {proxies_query_with_metro_area}
            """

        # fetch proxies
        proxies_result = await get_pg_readonly_database().fetch_all(
            query=proxies_query, values=proxies_values
        )
        proxies = [dict(r) for r in proxies_result]

        # fetch target metro coords (if provided)
        target_lat = target_lon = None
        if metro_area_id:
            target_row = await get_pg_readonly_database().fetch_one(
                query="SELECT latitude, longitude FROM ams.metro_area WHERE id = :id",
                values={"id": metro_area_id},
            )
            if target_row:
                row = dict(target_row)  # safe mapping
                target_lat = row.get("latitude")
                target_lon = row.get("longitude")
            else:
                target_lat = target_lon = None

        # compute distances in miles using haversine_distance helper
        for p in proxies:
            proxy_lat = p.get("proxy_lat")
            proxy_lon = p.get("proxy_lon")
            if (
                proxy_lat is not None
                and proxy_lon is not None
                and target_lat is not None
                and target_lon is not None
            ):
                try:
                    p["distance_miles"] = haversine_distance(
                        (float(proxy_lat), float(proxy_lon)),
                        (float(target_lat), float(target_lon)),
                    )
                except Exception:
                    p["distance_miles"] = None
            else:
                p["distance_miles"] = None

        # sort: 0 = same metro, 1 = nearest metros (with distance), 2 = others/unassigned
        def _sort_key(p):
            if p.get("proxy_metro") == metro_area_id:
                priority = 0
            elif p.get("distance_miles") is not None:
                priority = 1
            else:
                priority = 2
            dist = p["distance_miles"] if p["distance_miles"] is not None else 1e9
            return (priority, dist, p.get("proxy") or "")

        proxies.sort(key=_sort_key)

        available_proxies = [dict(item) for item in proxies]

        print(f"Available persons: {len(available_persons)}")
        print(f"Available emails: {len(available_emails)}")
        print(f"Available addresses: {len(available_addresses)}")
        print(f"Available phones: {len(available_phones)}")
        available_items = {
            "available_persons": available_persons,
            "available_emails": available_emails,
            "available_addresses": available_addresses,
            "available_phones": available_phones,
            "available_proxies": available_proxies
        }
        return available_items
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_available_persons_in_metro(
    metro_area_id: str, has_dob: bool = False, has_ssn: bool = False
):
    """
    Retrieve active persons not yet associated (via an account address) with the given metro area.

    The selection excludes any person who already has an account whose address
    belongs to the specified metro_area_id. Optional flags enforce presence of
    date_of_birth and/or last_4_ssn. When a flag is False its condition is a no-op.
    Result ordering is randomized.

    Args:
        metro_area_id (str): UUID of the metro area to exclude already-associated persons.
        has_dob (bool): If True, only include persons with non-null date_of_birth.
        has_ssn (bool): If True, only include persons with non-null last_4_ssn.

    Returns:
        list[dict]: Each dict contains:
            id (str): Person UUID.
            first_name (str)
            last_name (str)
            date_of_birth (datetime | None)

    Raises:
        HTTPException: 500 on database/query errors.
    """
    try:
        query = """
            SELECT
                p.id,
                p.first_name,
                p.last_name,
                p.date_of_birth
            FROM ams.ams_person p
            WHERE p.status = 'Active'
                AND (:has_dob = FALSE OR ( :has_dob = TRUE AND p.date_of_birth IS NOT NULL ))
                AND (:has_ssn = FALSE OR ( :has_ssn = TRUE AND p.last_4_ssn IS NOT NULL ))
                AND NOT EXISTS (
                    SELECT 1
                    FROM ams.ams_account a_inner
                    JOIN ams.ams_address addr ON a_inner.ams_address_id = addr.id
                    WHERE a_inner.ams_person_id = p.id
                        AND addr.metro_area_id = :metro_area_id
                )
            ORDER BY RANDOM();
                """
        persons_result = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"metro_area_id": metro_area_id, "has_dob": has_dob, "has_ssn": has_ssn},
        )
        persons = [dict(item) for item in persons_result]
        return persons
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_available_emails_by_person_ids(person_ids: list[str]):
    """
    Retrieve available (unassigned) email records for a set of person IDs.

    This selects emails whose ams_person_id is in the provided list. It does
    not currently filter by email status beyond the direct match; callers
    should ensure they only pass IDs of persons for which email availability
    is relevant.

    Args:
        person_ids (list[str]): List of person UUID strings to match against
            ams_email. If empty, the query will return an empty list.

    Returns:
        list[dict]: Each dict contains:
            email_id (str): The email UUID.
            ams_person_id (str): The owning person UUID.
            email_address (str): The email address string.

    Raises:
        HTTPException: 500 if a database error occurs.
    """
    try:
        query = """
            SELECT
                e.id as email_id,
                e.ams_person_id,
                e.email_address
            FROM ams.ams_email e
            WHERE e.ams_person_id = ANY(CAST(:person_ids AS UUID[]))
            AND e.status = 'AVAILABLE'
                """
        emails_result = await get_pg_readonly_database().fetch_all(
            query=query, values={"person_ids": person_ids}
        )
        emails = [dict(item) for item in emails_result]
        return emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_available_addresses_in_metro(metro_area_id: str):
    """
    Retrieve unused addresses for a specific metro area.

    An address is considered available if no account currently references it
    (i.e. there is no ams_account row whose ams_address_id = address.id). For each
    address, basic location fields plus a JSON object (metro_area) containing the
    metro name and state abbreviation are returned. Results are randomized.

    Args:
        metro_area_id (str): UUID of the metro area whose unassigned addresses
            should be listed.

    Returns:
        list[dict]: A list of address dictionaries. Each dict contains:
            id (str): Address UUID.
            street_one (str): Primary street line.
            street_two (str | None): Secondary street line.
            postal_code (str): Postal/ZIP code.
            metro_area (dict): Parsed object with:
                name (str): Metro area name.
                state (str): State abbreviation.

    Raises:
        HTTPException: 500 if a database error occurs.
    """
    try:
        query = """
            SELECT
                d.id,
                d.street_one,
                d.street_two,
                d.postal_code,
                d.city,
                addr_state.abbreviation as state_abbr,
                jsonb_build_object(
                    'id', ma.id,
                    'name', ma.name,
                    'state', ma_state.abbreviation
                ) AS metro_area
            FROM ams.ams_address d
            LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
            LEFT JOIN ams.state AS ma_state ON ma.state_id = ma_state.id
            LEFT JOIN ams.state AS addr_state ON d.state_id = addr_state.id
            LEFT JOIN ams.ams_account a ON d.id = a.ams_address_id
            WHERE d.metro_area_id = :metro_area_id  -- Get an address in the same metro area
            AND a.id IS NULL  -- Ensure the address is not already used in an account
            ORDER BY RANDOM()  -- Pick random available addresses
            """
        addresses_result = await get_pg_readonly_database().fetch_all(
            query=query, values={"metro_area_id": metro_area_id}
        )
        addresses = []
        for item in addresses_result:
            row = dict(item)
            ma = row.get("metro_area")
            if isinstance(ma, (str, bytes)):
                try:
                    if isinstance(ma, bytes):
                        ma = ma.decode("utf-8")
                    row["metro_area"] = json.loads(ma)
                except Exception:
                    row["metro_area"] = ma
            else:
                row["metro_area"] = ma
            addresses.append(row)

        return addresses
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_available_phones():
    """
    Retrieve active phone numbers not currently assigned to any account.

    A phone number is considered available if:
      * Its status is 'Active'.
      * No row in ams.ams_account references it via phone_number_id.

    Results are randomized (ORDER BY RANDOM()), which can be expensive on large
    tables; replace with a deterministic ORDER BY + LIMIT if performance degrades.

    Returns:
        list[dict]: Each dictionary contains:
            id (str): Phone number UUID.
            number (str): The phone number string.
            provider (str | None): Provider name (nullable).

    Raises:
        HTTPException: 500 if a database or query execution error occurs.
    """
    try:
        query = """
            SELECT
                ph.id,
                ph.number,
                ph.created_at,
                jsonb_build_object(
                    'code', php.code,
                    'label', php.label,
                    'type', php.type
                ) AS provider
            FROM ams.phone_number ph
            LEFT JOIN ams.ams_phone_provider php ON php.code = ph.provider_code
            WHERE ph.status = 'Active'
              AND ph.provider_code <> 'PERSONAL'
              AND NOT EXISTS (
                    SELECT 1
                    FROM ams.ams_account a
                    WHERE a.phone_number_id = ph.id
              )
            """
        phones_result = await get_pg_readonly_database().fetch_all(query=query)
        phones = []
        for item in phones_result:
            row = dict(item)
            provider = row.get("provider")
            if isinstance(provider, (str, bytes)):
                try:
                    if isinstance(provider, bytes):
                        provider = provider.decode("utf-8")
                    row["provider"] = json.loads(provider)
                except Exception:
                    row["provider"] = provider
            else:
                row["provider"] = provider
            phones.append(row)
        return phones
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_available_proxies():
    """
    Retrieve proxies that are currently available (status = 'AVAILABLE') and
    not assigned to any existing account.

    A proxy is considered available if:
      * Its status (case-insensitive) is 'AVAILABLE'.
      * No row in ams.ams_account references it via ams_proxy_id.

    Returns:
        list[dict]: Each proxy dictionary contains:
            id (str): Proxy UUID.
            proxy (str): Raw proxy string (e.g., host:port:user:pass or similar format).
            proxy_metro (str | None): Metro area UUID the proxy is associated with (may be null).
            provider_name (str | None): Name of the proxy provider (if linked).

    Raises:
        HTTPException: 500 if a database/query execution error occurs.
    """
    try:
        query = """
            SELECT
                p.id,
                p.proxy,
                p.proxy_metro,
                pp.name as provider_name
            FROM ams.ams_proxy p
            LEFT JOIN ams.ams_account a ON p.id = a.ams_proxy_id
            LEFT JOIN ams.ams_proxy_provider pp ON p.provider_id = pp.id
            WHERE p.status_code = 'AVAILABLE'
                AND a.id IS NULL  -- Find proxies not used in any account;
            """
        proxies_result = await get_pg_readonly_database().fetch_all(query=query)
        proxies = [dict(item) for item in proxies_result]
        return proxies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

async def get_all_proxies_with_status():
    """
    Retrieve all proxies with statuses AVAILABLE, IN-USE, and IN-USE RECYCLED.

    Returns proxies regardless of whether they are currently assigned to an account.
    Filters by status_code to include only:
      * 'AVAILABLE'
      * 'IN_USE'
      * 'IN_USE_RECYCLED'

    Returns:
        list[dict]: Each proxy dictionary contains:
            id (str): Proxy UUID.
            proxy (str): Raw proxy string (e.g., host:port:user:pass or similar format).
            proxy_metro (str | None): Metro area UUID the proxy is associated with (may be null).
            provider_name (str | None): Name of the proxy provider (if linked).
            status_code (str): Current status of the proxy.

    Raises:
        HTTPException: 500 if a database/query execution error occurs.
    """
    try:
        query = """
            SELECT
                p.id,
                p.proxy,
                p.proxy_metro,
                p.status_code,
                pp.name as provider_name
            FROM ams.ams_proxy p
            LEFT JOIN ams.ams_proxy_provider pp ON p.provider_id = pp.id
            WHERE p.status_code IN ('AVAILABLE', 'IN_USE', 'IN_USE_RECYCLED')
            """
        proxies_result = await get_pg_readonly_database().fetch_all(query=query)
        proxies = [dict(item) for item in proxies_result]
        return proxies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

async def get_next_account_increment(metro_area_id: str):
    """
    Compute the next numeric nickname increment for accounts in a metro.

    Nicknames follow the pattern: <metro_shortname><NNN>
    Example: ATL001, ATL002, etc. The query:
      * Finds the metro's shortname (metro_shortname).
      * Scans existing accounts (joined via addresses for that metro).
      * Extracts the last 3 trailing digits (if they match the pattern).
      * Determines the maximum existing numeric suffix.
      * Returns max_suffix + 1 as next_suffix (1-based; returns 1 if none exist).

    Args:
        metro_area_id (str): UUID of the metro area whose next increment you want.

    Returns:
        Record | None: Row-like object (or None if metro not found) with:
            metro_shortname (str): Metro shortname prefix.
            next_suffix (int): The next integer suffix (un-padded), e.g. 13 for ATL013.

    Raises:
        HTTPException: 500 if a database or execution error occurs.
    """
    try:
        query = """
            WITH base AS (
                SELECT
                    ma.metro_shortname,
                    COALESCE(
                        MAX(
                            CAST(SUBSTRING(a.nickname FROM '([0-9]{3})$') AS INT)
                        ),
                        0
                    ) AS max_suffix
                FROM ams.metro_area ma
                LEFT JOIN ams.ams_address addr ON addr.metro_area_id = ma.id
                LEFT JOIN ams.ams_account a
                    ON a.ams_address_id = addr.id
                    AND a.nickname ~ ('^' || ma.metro_shortname || '[0-9]{3}$')
                WHERE ma.id = :metro_area_id
                GROUP BY ma.metro_shortname
            )
            SELECT
                metro_shortname,
                (max_suffix + 1) AS next_suffix
            FROM base;
        """
        row = await get_pg_readonly_database().fetch_one(
            query=query, values={"metro_area_id": metro_area_id}
        )

        return dict(row) if row else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_account_items_by_metro_id_v2(
    metro_area_id: str,
    has_dob: bool = False,
    has_ssn: bool = False,
    create_mlx_profile: bool = False,
):
    """
    Aggregate account building-block resources for a specific metro (v2).

    Gathers (for one metro area) the available persons (optionally filtered
    by DOB / SSN presence), their available emails, unassigned addresses,
    available phones, proxies, grouped proxies (by metro + unassigned),
    account tags, and the next nickname increment. Also computes a
    readiness count based on how many complete person+email+address
    combinations can be formed.

    Args:
        metro_area_id (str): UUID of the target metro area whose resources
            should be collected.
        has_dob (bool, optional): If True, restrict persons to those with a
            non-null date_of_birth. Defaults to False.
        has_ssn (bool, optional): If True, restrict persons to those with a
            non-null last_4_ssn. Defaults to False.
        create_mlx_profiles (bool, optional): Whether to create MXL profiles
            for new accounts. Defaults to False.

    Returns:
        dict: Response payload with:
            metro (dict | None): Metadata for the requested metro (or None if not found).
            ready_count (int): Minimum of (persons_with_emails count) and (addresses count).
            persons (list[dict]): All candidate persons (may include those without emails).
            persons_with_emails (list[dict]): Subset of persons that have ≥1 available email; each enriched with 'emails'.
            addresses (list[dict]): Unused addresses in the metro.
            phones (list[dict]): Available phone numbers (not assigned).
            proxies (list[dict]): Flat list of available proxies.
            grouped_proxies (list[dict]): Proxies grouped by their metro (plus an 'Unassigned' group).
            tags (list[dict]): Account tag metadata.
            next_increment (Record | None): Next nickname increment info (metro_shortname + numeric suffix) or None.

    Raises:
        HTTPException: On database / retrieval errors (500).
    """
    metros = await get_all_metro_areas()
    single_metro = (
        list(filter(lambda m: str(m["id"]) == str(metro_area_id), metros))[0]
        if metro_area_id
        else None
    )
    proxies = await get_available_proxies()

    if create_mlx_profile:
        # Filter proxies to only those matching the metro_area_id
        proxies = [
            proxy for proxy in proxies
            if str(proxy["proxy_metro"]) == str(metro_area_id)
        ]

    grouped_proxies = []
    for metro in metros:
        grouped_proxies.append(
            {
                "metro_area": metro,
                "proxies": [
                    proxy for proxy in proxies if proxy["proxy_metro"] == metro["id"]
                ],
            }
        )
    grouped_proxies.append(
        {
            "metro_area": {"id": None, "name": "Unassigned"},
            "proxies": [proxy for proxy in proxies if not proxy["proxy_metro"]],
        }
    )
    grouped_proxies = [gp for gp in grouped_proxies if gp["proxies"]]
    persons = await get_available_persons_in_metro(metro_area_id, has_dob, has_ssn)
    emails = await get_available_emails_by_person_ids(
        [person["id"] for person in persons]
    )
    addresses = await get_available_addresses_in_metro(metro_area_id)
    phones = await get_available_phones()
    tags = await get_account_tags()
    next_increment = await get_next_account_increment(metro_area_id)

    persons_with_emails = []
    for person in persons:
        person_emails = [
            email for email in emails if email["ams_person_id"] == person["id"]
        ]
        if person_emails:
            person_copy = person.copy()
            person_copy["emails"] = person_emails
            persons_with_emails.append(person_copy)

    ready_count = min(len(persons_with_emails), len(addresses))

    if create_mlx_profile:
        # When creating MXL profiles, readiness also depends on available proxies and phones
        ready_count = min(len(persons_with_emails), len(addresses), len(proxies), len(phones))

    return {
        "metro": single_metro,
        "ready_count": ready_count,
        "persons": persons,
        "persons_with_emails": persons_with_emails,
        "addresses": addresses,
        "phones": phones,
        "proxies": proxies,
        "grouped_proxies": grouped_proxies,
        "tags": tags,
        "next_increment": next_increment,
    }


async def get_account_items_for_none_metro(
    none_metro_id: str,
    has_dob: bool = False,
    has_ssn: bool = False,
    create_mlx_profile: bool = False,
):
    """
    Aggregate account building-block resources regardless of metro area (v2).

    Gathers all available persons (optionally filtered by DOB / SSN presence), their
    available emails, all available addresses, phones, proxies, grouped proxies, and account tags.

    Args:
        has_dob (bool, optional): If True, restrict persons to those with a
            non-null date_of_birth. Defaults to False.
        has_ssn (bool, optional): If True, restrict persons to those with a
            non-null last_4_ssn. Defaults to False.
        create_mlx_profile (bool, optional): Whether to create MXL profiles
            for new accounts. Defaults to False.

    Returns:
        dict: Response payload with:
            metro (None): Always None for this case.
            ready_count (int): Minimum of (persons_with_emails count) and (addresses count).
            persons (list[dict]): All candidate persons (may include those without emails).
            persons_with_emails (list[dict]): Subset of persons that have ≥1 available email; each enriched with 'emails'.
            addresses (list[dict]): All unused addresses regardless of metro area.
            phones (list[dict]): Available phone numbers (not assigned).
            proxies (list[dict]): Flat list of available proxies.
            grouped_proxies (list[dict]): Proxies grouped by their metro (plus an 'Unassigned' group).
            tags (list[dict]): Account tag metadata.
            next_increment (None): Always None for this case (no shortname available).

    Raises:
        HTTPException: On database / retrieval errors (500).
    """
    metros = await get_all_metro_areas()
    none_metro = list(filter(lambda m: str(m["id"]) == str(none_metro_id), metros))[0]
    proxies = await get_all_proxies_with_status()

    grouped_proxies = []
    for metro in metros:
        grouped_proxies.append(
            {
                "metro_area": metro,
                "proxies": [
                    proxy
                    for proxy in proxies
                    if proxy["proxy_metro"] == metro["id"]
                    and proxy["status_code"] == "AVAILABLE"
                ],
            }
        )
    grouped_proxies.append(
        {
            "metro_area": {"id": None, "name": "Unassigned"},
            "proxies": [
                proxy
                for proxy in proxies
                if not proxy["proxy_metro"] and proxy["status_code"] == "AVAILABLE"
            ],
        }
    )
    grouped_proxies = [gp for gp in grouped_proxies if gp["proxies"]]

    persons = await get_all_available_persons(has_dob, has_ssn)
    emails = await get_available_emails_by_person_ids(
        [person["id"] for person in persons]
    )

    addresses = await get_all_addresses()
    phones = await get_available_phones()
    tags = await get_account_tags()
    next_increment = await get_next_account_increment(none_metro_id)

    persons_with_emails = []
    for person in persons:
        person_emails = [
            email for email in emails if email["ams_person_id"] == person["id"]
        ]
        if person_emails:
            person_copy = person.copy()
            person_copy["emails"] = person_emails
            persons_with_emails.append(person_copy)

    ready_count = min(len(persons_with_emails), len(addresses))

    if create_mlx_profile:
        # When creating MXL profiles, readiness also depends on available proxies and phones
        ready_count = min(len(persons_with_emails), len(addresses), len(proxies), len(phones))

    return {
        "metro": none_metro,
        "ready_count": ready_count,
        "persons": persons,
        "persons_with_emails": persons_with_emails,
        "addresses": addresses,
        "phones": phones,
        "proxies": proxies,
        "grouped_proxies": grouped_proxies,
        "tags": tags,
        "next_increment": next_increment,
    }


async def get_all_available_persons(has_dob: bool = False, has_ssn: bool = False):
    """
    Retrieve all active persons regardless of metro area.

    Optional flags enforce presence of date_of_birth and/or last_4_ssn.
    Result ordering is randomized.

    Args:
        has_dob (bool): If True, only include persons with non-null date_of_birth.
        has_ssn (bool): If True, only include persons with non-null last_4_ssn.

    Returns:
        list[dict]: Each dict contains:
            id (str): Person UUID.
            first_name (str)
            last_name (str)
            date_of_birth (datetime | None)

    Raises:
        HTTPException: 500 on database/query errors.
    """
    try:
        query = """
            SELECT
                p.id,
                p.first_name,
                p.last_name,
                p.date_of_birth
            FROM ams.ams_person p
            WHERE p.status = 'Active'
                AND (:has_dob = FALSE OR ( :has_dob = TRUE AND p.date_of_birth IS NOT NULL ))
                AND (:has_ssn = FALSE OR ( :has_ssn = TRUE AND p.last_4_ssn IS NOT NULL ))
            ORDER BY RANDOM();
                """
        persons_result = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"has_dob": has_dob, "has_ssn": has_ssn},
        )
        persons = [dict(item) for item in persons_result]
        return persons
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_addresses():
    """
    Retrieve all addresses regardless of metro area assignment.

    An address is considered available if no account currently references it
    (i.e. there is no ams_account row whose ams_address_id = address.id).
    Results are randomized.

    Returns:
        list[dict]: A list of address dictionaries. Each dict contains:
            id (str): Address UUID.
            street_one (str): Primary street line.
            street_two (str | None): Secondary street line.
            postal_code (str): Postal/ZIP code.
            metro_area (dict | None): Metro area info or None if not assigned.

    Raises:
        HTTPException: 500 if a database error occurs.
    """
    try:
        query = """
            SELECT
                d.id,
                d.street_one,
                d.street_two,
                d.postal_code,
                d.city,
                ds.abbreviation as state_abbr,
                jsonb_build_object(
                    'id', ma.id,
                    'name', ma.name,
                    'state', ms.abbreviation
                ) AS metro_area
            FROM ams.ams_address d
            LEFT JOIN ams.state AS ds ON d.state_id = ds.id
            LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
            LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
            ORDER BY RANDOM()
            """
        addresses_result = await get_pg_readonly_database().fetch_all(query=query)
        addresses = []
        for item in addresses_result:
            row = dict(item)
            ma = row.get("metro_area")
            if isinstance(ma, (str, bytes)):
                try:
                    if isinstance(ma, bytes):
                        ma = ma.decode("utf-8")
                    row["metro_area"] = json.loads(ma)
                except Exception:
                    row["metro_area"] = ma
            else:
                row["metro_area"] = ma
            addresses.append(row)
        return addresses
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def check_account_nickname_unique(nickname: str):
    """
    Determine whether the provided account nickname is unused.

    Args:
        nickname (str): Exact nickname string to test for uniqueness.

    Returns:
        bool: True if no existing account has this nickname; False otherwise.

    Raises:
        HTTPException: If a database or execution error occurs.
    """
    try:
        query = """
            SELECT COUNT(*) AS count
            FROM ams.ams_account
            WHERE nickname = :nickname;
        """
        result = await get_pg_readonly_database().fetch_one(
            query=query, values={"nickname": nickname}
        )
        count = result["count"] if result else 0
        return count == 0  # Return True if unique, False otherwise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_account_items_in_any_metro(
    quantity: int = 1,
    timezone_id: str = None,
    has_dob: bool = False,
    has_ssn: bool = False
):
    timezone_condition = ""
    values = {}
    if timezone_id:
        timezone_condition = "AND tz.id = :timezone_id"
        values["timezone_id"] = timezone_id

    try:
        # Get available addresses with metro information
        addresses_query = f"""
            SELECT 
                d.id, 
                d.street_one, 
                d.street_two, 
                d.postal_code,
                ma.id as metro_area_id,
                ma.name as metro_name,
                ms.abbreviation as state,
                c.name as country,
                tz.abbreviation as timezone
            FROM ams.ams_address d
            LEFT JOIN ams.ams_account AS a ON d.id = a.ams_address_id
            LEFT JOIN ams.metro_area AS ma ON d.metro_area_id = ma.id
            LEFT JOIN ams.timezone AS tz ON ma.timezone = tz.id::TEXT
            LEFT JOIN ams.country AS c ON ma.country_id = c.id
            LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
            WHERE a.id IS NULL
            AND ma.id IS NOT NULL
            {timezone_condition}
            ORDER BY RANDOM()
        """

        addresses_result = await get_pg_readonly_database().fetch_all(
            query=addresses_query, values=values
        )
        available_addresses = [dict(item) for item in addresses_result]

        # Build filtering conditions for DOB and SSN
        dob_ssn_conditions = ""
        if has_dob and has_ssn:
            dob_ssn_conditions = "AND p.date_of_birth IS NOT NULL AND p.last_4_ssn IS NOT NULL"
        elif has_dob:
            dob_ssn_conditions = "AND p.date_of_birth IS NOT NULL"
        elif has_ssn:
            dob_ssn_conditions = "AND p.last_4_ssn IS NOT NULL"

        # Get available persons with their occupied metro IDs
        persons_query = f"""
            WITH person_metros AS (
                SELECT 
                    p.id,
                    p.first_name,
                    p.last_name,
                    p.date_of_birth,
                    COALESCE(
                        ARRAY_AGG(DISTINCT addr.metro_area_id) FILTER (WHERE addr.metro_area_id IS NOT NULL), 
                        ARRAY[]::UUID[]
                    ) as occupied_metro_ids
                FROM ams.ams_person p
                LEFT JOIN ams.ams_account a ON a.ams_person_id = p.id
                LEFT JOIN ams.ams_address addr ON a.ams_address_id = addr.id
                WHERE p.status = 'Active'
                {dob_ssn_conditions}
                GROUP BY p.id, p.first_name, p.last_name, p.date_of_birth, p.last_4_ssn
            ),
            available_metro_ids AS (
                SELECT DISTINCT addr_available.metro_area_id
                FROM ams.ams_address addr_available
                LEFT JOIN ams.metro_area ma ON addr_available.metro_area_id = ma.id
                LEFT JOIN ams.timezone tz ON ma.timezone = tz.id::TEXT
                WHERE addr_available.metro_area_id IS NOT NULL
                AND addr_available.id NOT IN (
                    SELECT ams_address_id 
                    FROM ams.ams_account 
                    WHERE ams_address_id IS NOT NULL
                )
                {timezone_condition}
            )
            SELECT 
                pm.id,
                pm.first_name,
                pm.last_name,
                pm.date_of_birth,
                pm.occupied_metro_ids
            FROM person_metros pm
            WHERE EXISTS (
                SELECT 1 FROM available_metro_ids ami
                WHERE NOT (ami.metro_area_id = ANY(pm.occupied_metro_ids))
            )
            ORDER BY pm.first_name, pm.last_name, pm.date_of_birth, pm.id
        """

        persons_result = await get_pg_readonly_database().fetch_all(
            query=persons_query, values=values
        )
        available_persons = [dict(item) for item in persons_result]

        # Get available emails
        emails_query = """
            SELECT
                e.id,
                e.email_address
            FROM ams.ams_email e
            LEFT JOIN ams.ams_account a ON e.id = a.ams_email_id
            WHERE UPPER(e.status::varchar) = 'AVAILABLE'
                AND a.id IS NULL
            ORDER BY RANDOM()
        """
        emails_result = await get_pg_readonly_database().fetch_all(query=emails_query)
        available_emails = [dict(item) for item in emails_result]

        # Get available phones
        phones_query = """
            SELECT 
                ph.id,
                ph.number
            FROM ams.phone_number ph
            LEFT JOIN ams.ams_account a ON ph.id = a.phone_number_id
            WHERE a.id IS NULL
            AND ph.status = 'Active'
            ORDER BY RANDOM()
        """
        phones_result = await get_pg_readonly_database().fetch_all(query=phones_query)
        available_phones = [dict(phone) for phone in phones_result]

        proxies_query = """
            SELECT
                p.id,
                p.proxy
            FROM ams.ams_proxy p
            LEFT JOIN ams.ams_account a ON p.id = a.ams_proxy_id
            WHERE p.status_code = 'AVAILABLE'
                AND a.id IS NULL  -- Find proxies not used in any account
            ORDER BY RANDOM();
            """

        proxies_result = await get_pg_readonly_database().fetch_all(query=proxies_query)
        available_proxies = [dict(item) for item in proxies_result]

        return {
            "available_emails": available_emails,
            "available_phones": available_phones,
            "available_persons": available_persons,
            "available_addresses": available_addresses,
            "available_proxies": available_proxies
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_accounts(accounts: List[AccountRequestModel]):
    """
    DEPRECATED ---
    Create accounts with their associated completed steps in a transaction.
    Uses batch operations for optimal performance.
    """
    try:
        async with get_pg_database().transaction():
            created_accounts = []
            proxy_ids_to_update = []

            for account in accounts:
                # Step 1: Insert account and get the generated ID
                account_query = """
                    INSERT INTO ams.ams_account (
                        id, nickname, ams_person_id, ams_email_id, ams_address_id,
                        ams_proxy_id, phone_number_id, company_id, status, notes
                    ) VALUES (
                        uuid_generate_v4(), :nickname, :ams_person_id, :ams_email_id, 
                        :ams_address_id, :ams_proxy_id, :phone_number_id, :company_id, :status, :notes
                    ) RETURNING id, nickname
                """

                account_values = {
                    "nickname": account.nickname,
                    "ams_person_id": account.ams_person_id,
                    "ams_email_id": account.ams_email_id,
                    "ams_address_id": account.ams_address_id,
                    "ams_proxy_id": account.ams_proxy_id,
                    "phone_number_id": account.phone_number_id,
                    "company_id": account.company_id,
                    "status": "Active",
                    "notes": account.notes
                }

                account_result = await get_pg_database().fetch_one(
                    query=account_query,
                    values=account_values
                )
                account_id = account_result["id"]
                if account.ams_proxy_id:
                    proxy_ids_to_update.append(account.ams_proxy_id)

                created_accounts.append({
                    "id": account_id,
                    "nickname": account_result["nickname"],
                    "steps_count": len(account.completed_steps) if account.completed_steps else 0
                })

                # Step 3: Update proxy status to 'In Use' for all assigned proxies
            if proxy_ids_to_update:
                # Remove duplicates while preserving order
                unique_proxy_ids = list(dict.fromkeys(proxy_ids_to_update))
                # Use the existing update_proxies_status function
                update_data = {
                    'proxy_ids': unique_proxy_ids,
                    'status': 'IN_USE'
                }
                await update_proxies_status(update_data)

            return created_accounts
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


async def create_accounts_v2(accounts: List[AccountRequestModelV2]):
    """
    Bulk create multiple AMS accounts (v2 schema) and attach tag mappings in one operation.

    Builds a single multi-value INSERT statement for accounts, executes it, then (if
    any tags were supplied) performs a second batched INSERT into account_tag_mapping.
    Tag insertion is idempotent per (account_id, tag_id) due to ON CONFLICT DO NOTHING.

    Args:
        accounts (List[AccountRequestModelV2]): Sequence of validated account request
            models. Each item should include required foreign keys
            (ams_person_id, ams_email_id, ams_address_id, etc.), a nickname, and
            optionally a list of tag UUIDs in .tags.

    Returns:
        list[Mapping]: List of rows returned by the INSERT .. RETURNING clause.
            Each row contains:
                id (UUID): Generated account UUID.
                nickname (str): The persisted nickname.

    Raises:
        HTTPException: 
            500 if any database error occurs during account or tag insertion.

    """
    building_status_id = await get_pg_database().fetch_val(
        "SELECT id FROM ams.ams_account_status WHERE code = 'BUILDING'"
    )

    values_sql = []
    params = []

    for idx, acc in enumerate(accounts):
        values_sql.append(
            f"""
            (uuid_generate_v4(), :nickname{idx}, :ams_person_id{idx}, :ams_email_id{idx},
             :ams_address_id{idx}, :ams_proxy_id{idx}, :phone_number_id{idx},
             :company_id{idx}, :status_code{idx}, :status_id)
            """
        )
        params.extend(
            {
                f"nickname{idx}": acc.nickname,
                f"ams_person_id{idx}": acc.ams_person_id,
                f"ams_email_id{idx}": acc.ams_email_id,
                f"ams_address_id{idx}": acc.ams_address_id,
                f"ams_proxy_id{idx}": acc.ams_proxy_id,
                f"phone_number_id{idx}": acc.phone_number_id,
                f"company_id{idx}": acc.company_id,
                f"status_code{idx}": "BUILDING",
                "status_id": building_status_id,
            }.items()
        )
    values_sql_str = ", ".join(values_sql)
    query = f"""
        INSERT INTO ams.ams_account (
            id, nickname, ams_person_id, ams_email_id, ams_address_id,
            ams_proxy_id, phone_number_id, company_id, status_code, status_id
        ) VALUES {values_sql_str}
        RETURNING id, nickname;
    """
    try:
        result = await get_pg_database().fetch_all(query=query, values=dict(params))
        account_tag_rows = []
        proxy_rows = []
        automator_rows = []
        pos_rows = []
        email_rows = []
        phone_number_rows = []
        for acc_model, acc_row in zip(accounts, result):
            if acc_model.tags:
                for tag_id in acc_model.tags:
                    account_tag_rows.append(
                        {
                            "id": str(uuid.uuid4()),
                            "account_id": acc_row["id"],
                            "tag_id": tag_id,
                        }
                    )
            if acc_model.ams_proxy_id:
                proxy_rows.append(acc_model.ams_proxy_id)
            if acc_model.automator:
                automator_rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "account_id": acc_row["id"],
                        "automator_id": acc_model.automator
                    }
                )
            if acc_model.point_of_sale:
                pos_rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "account_id": acc_row["id"],
                        "point_of_sale_id": acc_model.point_of_sale
                    }
                )
            if acc_model.ams_email_id:
                email_rows.append(acc_model.ams_email_id)
            if acc_model.phone_number_id:
                phone_number_rows.append(acc_model.phone_number_id)

        if account_tag_rows:
            mapping_values_sql = ", ".join(
                f"(:id{i}, :account_id{i}, :tag_id{i}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                for i in range(len(account_tag_rows))
            )
            insert_tags_sql = f"""
                INSERT INTO ams.account_tag_mapping (id, account_id, tag_id, created_at, last_modified)
                VALUES {mapping_values_sql}
                ON CONFLICT (account_id, tag_id) DO NOTHING;
            """
            flat = {}
            for i, row in enumerate(account_tag_rows):
                flat[f"id{i}"] = row["id"]
                flat[f"account_id{i}"] = row["account_id"]
                flat[f"tag_id{i}"] = row["tag_id"]
            await get_pg_database().execute(query=insert_tags_sql, values=flat)

        if proxy_rows:
            await update_proxies_status(
                {
                    "proxy_ids": list(set(proxy_rows)),
                    "status": "IN_USE"
                }
            )

        if automator_rows:
            for row in automator_rows:
                await update_account_automators(row["account_id"], [row["automator_id"]])

        if pos_rows:
            for row in pos_rows:
                await update_account_point_of_sale(row["account_id"], [row["point_of_sale_id"]])

        if email_rows:
            await update_emails_status(
                {
                    "email_ids": list(set(email_rows)),
                    "status": "IN USE"
                }
            )

        if phone_number_rows:
            await update_phone_numbers_status(
                {
                    "phone_ids": list(set(phone_number_rows)),
                    "status": "In-Use"
                }
            )

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def rebuild_accounts(accounts: List[RebuildAccountRequestItem]):
    """
    Rebuild multiple AMS accounts by creating new entries based on old ones,
    linking all specified resources, removing the 'Rebuild' tag from old accounts,
    and updating notes for both old and new accounts.
    """
    database = get_pg_database()
    current_step = "initialization"
    current_account_nickname = None
    current_old_account_id = None

    logger.info(f"Starting rebuild_accounts for {len(accounts)} account(s)")

    try:
        current_step = "fetching building status"
        building_status_id = await database.fetch_val(
            "SELECT id FROM ams.ams_account_status WHERE code = 'BUILDING'"
        )
        if not building_status_id:
            raise ValueError("BUILDING status not found in ams_account_status table")
        
        current_step = "fetching rebuild tag"
        rebuild_tag_result = await database.fetch_all(
            "SELECT id FROM ams.ams_account_tag WHERE name = 'Rebuild'"
        )
        rebuild_tag_id = rebuild_tag_result[0]["id"] if rebuild_tag_result else None
        logger.info(f"Rebuild tag ID: {rebuild_tag_id}")

        created_accounts = []

        async with database.transaction():
            for idx, acc in enumerate(accounts):
                current_old_account_id = acc.old_account_id
                current_account_nickname = acc.new_nickname
                logger.info(f"Processing account [{idx + 1}/{len(accounts)}]: old_id={acc.old_account_id}, new_nickname={acc.new_nickname}")

                # 1. Fetch old account details
                current_step = "fetching old account details"
                old_acc = await database.fetch_one(
                    "SELECT nickname, notes FROM ams.ams_account WHERE id = :id",
                    {"id": acc.old_account_id}
                )
                if not old_acc:
                    logger.warning(f"Old account {acc.old_account_id} not found, skipping")
                    continue

                old_nickname = old_acc["nickname"]
                old_notes = old_acc["notes"] or ""
                logger.info(f"Old account found: nickname={old_nickname}")

                # 2. Create new account
                current_step = "creating new account"
                new_account_id = str(uuid.uuid4())
                new_notes = f"from \"{old_nickname}\""
                if old_notes:
                    new_notes = f"{new_notes}\n{old_notes}"

                await database.execute(
                    """
                    INSERT INTO ams.ams_account (
                        id, nickname, ams_person_id, ams_email_id, ams_address_id,
                        ams_proxy_id, phone_number_id, company_id, status_code, status_id, notes
                    ) VALUES (
                        :id, :nickname, :ams_person_id, :ams_email_id, :ams_address_id,
                        :ams_proxy_id, :phone_number_id, :company_id, 'BUILDING', :status_id, :notes
                    )
                    """,
                    {
                        "id": new_account_id,
                        "nickname": acc.new_nickname,
                        "ams_person_id": acc.ams_person_id,
                        "ams_email_id": acc.email_id,
                        "ams_address_id": acc.ams_address_id,
                        "ams_proxy_id": acc.ams_proxy_id,
                        "phone_number_id": acc.phone_number_id,
                        "company_id": acc.company_id,
                        "status_id": building_status_id,
                        "notes": new_notes
                    }
                )
                logger.info(f"Created new account: id={new_account_id}, nickname={acc.new_nickname}")

                # 3. Associate Email
                if acc.email_id:
                    current_step = "updating email status"
                    logger.info(f"Updating email status for email_id={acc.email_id}")
                    
                    # Update email status directly in the same transaction
                    await database.execute(
                        """
                        UPDATE ams.ams_email
                        SET status = 'IN USE', last_modified = CURRENT_TIMESTAMP
                        WHERE id = :email_id
                        """,
                        {"email_id": acc.email_id}
                    )

                # 4. Associate Phone
                if acc.phone_number_id:
                    current_step = "updating phone status"
                    logger.info(f"Updating phone status for phone_id={acc.phone_number_id}")
                    
                    # Update phone status directly in the same transaction
                    await database.execute(
                        """
                        UPDATE ams.phone_number
                        SET status = 'In-Use', account_id = :acc_id
                        WHERE id = :phone_id
                        """,
                        {"acc_id": new_account_id, "phone_id": acc.phone_number_id}
                    )

                # 5. Associate Automator & POS (pass db to avoid nested transactions)
                if acc.automator_id:
                    current_step = "associating automator"
                    logger.info(f"Associating automator_id={acc.automator_id}")
                    await update_account_automators(new_account_id, [acc.automator_id], db=database)

                if acc.point_of_sale_id:
                    current_step = "associating point of sale"
                    logger.info(f"Associating point_of_sale_id={acc.point_of_sale_id}")
                    await update_account_point_of_sale(new_account_id, [acc.point_of_sale_id], db=database)

                # 6. Associate Tags
                if acc.tags:
                    current_step = "associating tags"
                    logger.info(f"Associating {len(acc.tags)} tag(s)")
                    for tag_id in acc.tags:
                        await database.execute(
                            """
                            INSERT INTO ams.account_tag_mapping (id, account_id, tag_id, created_at, last_modified)
                            VALUES (:id, :account_id, :tag_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (account_id, tag_id) DO NOTHING
                            """,
                            {"id": str(uuid.uuid4()), "account_id": new_account_id, "tag_id": tag_id}
                        )

                # 7. Remove "Rebuild" Tag from Old Account
                if rebuild_tag_id:
                    current_step = "removing rebuild tag from old account"
                    await database.execute(
                        "DELETE FROM ams.account_tag_mapping WHERE account_id = :acc_id AND tag_id = :tag_id",
                        {"acc_id": acc.old_account_id, "tag_id": rebuild_tag_id}
                    )

                # 8. Update Old Account Note
                current_step = "updating old account notes"
                rebuilt_note = f"rebuilt on \"{acc.new_nickname}\""
                updated_old_notes = f"{old_notes}\n{rebuilt_note}" if old_notes else rebuilt_note
                await database.execute(
                    "UPDATE ams.ams_account SET notes = :notes, last_modified = CURRENT_TIMESTAMP WHERE id = :id",
                    {"notes": updated_old_notes, "id": acc.old_account_id}
                )

                created_accounts.append({
                    "id": new_account_id,
                    "nickname": acc.new_nickname
                })
                logger.info(f"Successfully rebuilt account [{idx + 1}/{len(accounts)}]: {acc.new_nickname}")

        logger.info(f"Successfully rebuilt {len(created_accounts)} account(s)")
        return created_accounts

    except HTTPException:
        raise
    except Exception as e:
        error_context = (
            f"Failed at step '{current_step}' "
            f"for account '{current_account_nickname or 'unknown'}' "
            f"(old_id: {current_old_account_id or 'unknown'})"
        )
        logger.error(f"rebuild_accounts error: {error_context}. Error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"{error_context}. Error: {str(e)}"
        )


async def get_all_accounts():
    try:
        query = """
            SELECT * from ams.ams_account
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_accounts_with_null_proxy_id():
    try:
        query = """
            SELECT a.nickname, a.id FROM ams.ams_account a
            WHERE ams_proxy_id IS NULL
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_emails():
    try:
        query = """
            SELECT * from ams.ams_email
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_email_addresses():
    try:
        query = """
            SELECT id, email_address
            FROM ams.ams_email
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_persons_all():
    try:
        query = """
            SELECT * from ams.ams_person
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_searched_phone_numbers(
        page: int = 1,
        page_size: int = 10,
        sort_field: str | None = None,
        sort_order: str = 'desc',
        search_query: str = "",
        assigned_to_account: Literal["all", "yes", "no"] = "all",
        provider: Literal["All", "Text Chest", "Verizon", "T-Mobile", "US Mobile", "Tello", "Personal", "WiredSMS"] = "All",
        timezone: str = 'America/Chicago',
        status: Optional[Annotated[str, Literal['Active', 'Cancelled', 'Special']]] = None
):
    offset = (page - 1) * page_size
    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    if not timezone:
        timezone = "America/Chicago"

    try:
        timezone = pytz_timezone(timezone)
    except UnknownTimeZoneError:
        return HTTPException(status_code=400,
                             detail=f"Unknown timezone: {timezone}. Please provide a valid timezone string.")

    # Validate sort_field to prevent SQL injection and handle joins
    sortable_columns = [
        "created_at",
        "is_starred",
        "last_modified",
        "status",
        "nickname",
        "number",
        "provider",
        "last_four"
    ]
    if sort_field not in sortable_columns:
        sort_field = "last_modified"  # Default to a safe column
    order_clause = f"{sort_field} {sort_order}"
    try:
        query = f"""
            WITH filter_results AS (
                SELECT DISTINCT ON (p.id)
                    p.id, p.number, p.last_four, p.status, 
                    p.last_modified AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_modified,
                    p.created_at, p.notes, p.is_starred,
                    ac.id as account_id, ac.nickname,
                    jsonb_build_object(
                     'code',php.code,
                     'label',php.label,
                     'type',php.type
                    ) AS provider
                FROM ams.phone_number AS p
                LEFT JOIN ams.ams_account AS ac ON p.id = ac.phone_number_id
                LEFT JOIN ams.ams_phone_provider as php ON p.provider_code=php.code
                WHERE number ILIKE '%' || :search_query || '%'
                AND (:assigned_to_account = 'all' OR
						(:assigned_to_account = 'yes' AND ac.phone_number_id IS NOT NULL) OR
						(:assigned_to_account = 'no' AND ac.phone_number_id IS NULL))
                AND (:provider = '' OR php.label::text = :provider)
				AND (:status = '' OR p.status::text = :status)
            )
            SELECT *, (SELECT COUNT(*) FROM filter_results) AS total_count
            FROM filter_results
            ORDER BY {order_clause}
            LIMIT :page_size OFFSET :offset;
            """
        result = await get_pg_readonly_database().fetch_all(
            query=query, values={
                "search_query": search_query,
                "page_size": page_size,
                "offset": offset,
                "assigned_to_account": assigned_to_account,
                "provider": provider if provider != "All" else "",
                "timezone": timezone.zone,
                "status": status if status else ""
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def check_phone_number_unique(number: str):
    try:
        query = """
            SELECT ph.number, php.label as provider
            FROM ams.phone_number ph
            LEFT JOIN ams.ams_phone_provider php ON ph.provider_code = php.code
            WHERE ph.number = :number;
            """
        result = await get_pg_readonly_database().fetch_one(query=query, values={"number": number})
        if result:
            return {"is_unique": False, "dupe_data": dict(result)}
        else:
            return {"is_unique": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_proxies():
    try:
        query = """
            SELECT * FROM ams.ams_proxy
            ORDER BY proxy ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_phone_numbers(phone_numbers: list[PhoneNumberRequestModel]):
    try:
        query = """
            INSERT INTO ams.phone_number (
                id, number, provider_code, provider_id, created_at, created_by, status, account_id, notes)
            VALUES (
                uuid_generate_v4(), :number, :provider_code, 
                (SELECT id FROM ams.ams_phone_provider WHERE code = :provider_code), 
                :created_at, :created_by, :status, :account_id, :notes);
            """
        current_timestamp = datetime.now()
        values = [{
            **number.model_dump(),
            "created_at": datetime.fromisoformat(
                number.created_at.replace("Z", "")) if number.created_at else current_timestamp,
        } for number in phone_numbers]

        await get_pg_database().execute_many(query=query, values=values)
        return {"inserted_count": len(phone_numbers), "message": "Phone Numbers created successfully."}
    except UniqueViolationError:  # Handle unique constraint violation
        raise HTTPException(status_code=400, detail="A phone number with these details already exists.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_phone_number(phone_id: str, new_phone_number: PhoneNumberRequestModel):
    try:
        query = """
            UPDATE ams.phone_number ph
            SET 
                number = :number,
                provider_code = :provider_code,
                provider_id = pro.id,
                status = :status,
                created_at = :created_at,
                created_by = :created_by,
                notes = :notes,
                last_modified = CURRENT_TIMESTAMP
            FROM ams.ams_phone_provider pro
            WHERE pro.code = :provider_code
                AND ph.id = :phone_id
            RETURNING ph.*;
            """

        values = {
            **new_phone_number.model_dump(),
            "created_at": datetime.fromisoformat(new_phone_number.created_at.replace("Z", "")),
            "phone_id": phone_id
        }

        result = await get_pg_database().fetch_one(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_phone_number(phone_id: str):
    try:
        query = """
            SELECT ph.id, ph.account_id, ph.created_at, ph.number,
             ph.is_starred, ph.last_four, ph.last_modified, ph.notes, ph.provider_acct, ph.status,
             jsonb_build_object(
                'code', php.code,
                'label', php.label,
                'type', php.type
             ) AS provider
            FROM ams.phone_number ph
            LEFT JOIN ams.ams_phone_provider php ON ph.provider_code = php.code
            WHERE ph.id = :phone_id
            """
        result = await get_pg_readonly_database().fetch_one(
            query=query, values={"phone_id": phone_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_phone_providers():
    try:
        query = """
            SELECT * FROM ams.ams_phone_provider
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_phone_numbers_status(update_data: dict[str, list[str] | str]):
    try:
        phone_ids = update_data['phone_ids']

        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(phone_ids))])

        query = f"""
        UPDATE ams.phone_number
        SET
            status = :status,
            last_modified = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        RETURNING id;
        """

        # Create values dict with individual ID parameters
        values = {"status": update_data['status']}
        for i, pid in enumerate(phone_ids):
            values[f"id{i}"] = pid

        result = await get_pg_database().fetch_all(
            query=query,
            values=values
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_phone_star(phone_id: str):
    try:
        query = f"""
        UPDATE ams.phone_number
        SET
            is_starred = NOT is_starred
        WHERE id = :phone_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"phone_id": phone_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_proxies_by_ids(ids: list[str]):
    try:
        id_list = ", ".join([f"'{id}'" for id in ids])
        query = f"""
            SELECT * FROM ams.ams_proxy
            WHERE id IN ({id_list})
        """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_searched_proxies(
        page: int = 1,
        page_size: int = 10,
        sort_field: str = None,
        sort_order: str = 'desc',
        search_query: str = "",
        metro_area_ids: list[str] = None,
        timezone: str = None,
        provider_id: str = "",
        status: Optional[Literal['Available', 'In-Use', 'Replaced', 'Retired']] = None
):
    offset = (page - 1) * page_size
    # Validate sort order
    if not sort_order or sort_order.lower() not in ["asc", "desc"]:
        sort_order = "DESC"
    else:
        sort_order = sort_order.upper()

    # Validate sort_field to prevent SQL injection and handle joins
    sortable_columns = [
        "proxy",
        "provider_name",
        "zone",
        "proxy_metro_name",
        "account_name",
        "is_starred",
        "last_modified",
        "status",
    ]
    if sort_field not in sortable_columns:
        sort_field = "last_modified"  # Default to a safe column
    order_clause = f"{sort_field} {sort_order}"

    try:
        # Build the metro area filter condition
        metro_filter = ""
        if metro_area_ids and len(metro_area_ids) > 0:
            # Create placeholders for each metro area ID
            placeholders = ",".join([f"'{id}'" for id in metro_area_ids])
            metro_filter = f"AND ma.id IN ({placeholders})"

        query = f"""
            WITH filter_results AS (
                SELECT 
                    p.id, p.proxy, p.zone, p.status_code,
                    p.last_modified AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_modified,
                    p.is_starred, p.notes,
                    jsonb_build_object(
                        'id', ma.id,
                        'name', ma.name,
                        'country', c.name,
                        'state', ms.abbreviation,
                        'timezone', jsonb_build_object(
                            'id', t.id,
                            'name', t.name,
                            'abbreviation', t.abbreviation
                        )
                    ) AS proxy_metro,
                    jsonb_build_object(
                        'id', pd.id,
                        'name', pd.name
                    ) AS provider,
                    jsonb_build_object(
                        'id', acc.id,
                        'nickname', acc.nickname
                    ) AS account,
                    ma.name AS proxy_metro_name,  -- Include this for sorting
                    pd.name AS provider_name,  -- Include this for sorting
                    acc.nickname AS account_name  -- Include this for sorting
                FROM ams.ams_proxy AS p
                JOIN ams.ams_proxy_provider AS pd ON p.provider_id = pd.id
                LEFT JOIN ams.metro_area AS ma ON p.proxy_metro = ma.id
                LEFT JOIN ams.country AS c ON ma.country_id = c.id
                LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
                LEFT JOIN ams.timezone AS t ON ma.timezone = t.id::TEXT
                LEFT JOIN ams.ams_account AS acc ON acc.ams_proxy_id = p.id
                WHERE 
                    (COALESCE(p.proxy, '') || ' ' || COALESCE(acc.nickname, '') || COALESCE(p.id::text, '')) ILIKE '%' || :search_query || '%'
                AND (CASE
                         WHEN :provider_id = '' THEN TRUE
                         ELSE p.provider_id = CAST(:provider_id AS UUID)
                    END)
                AND (:status = '' OR p.status_code = :status)
                {metro_filter}
            )
            SELECT *, (SELECT COUNT(*) FROM filter_results) AS total_count
            FROM filter_results
            ORDER BY {order_clause}
            LIMIT :page_size OFFSET :offset;
            """

        values: Dict[str, Any] = {
            "search_query": search_query,
            "page_size": page_size,
            "offset": offset,
            "provider_id": provider_id,
            "timezone": timezone,
            "status": status if status else ""
        }
        result = await get_pg_readonly_database().fetch_all(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_proxy_providers():
    try:
        query = """
            SELECT id, name FROM ams.ams_proxy_provider
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_profiles():
    try:
        query = """
            SELECT p.id, p.profile_name, e.email_address
            FROM ams.ams_profile AS p
            LEFT JOIN ams.ams_email AS e ON p.ams_email_id = e.id
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_automators():
    try:
        query = """
            SELECT id, name, brand
            FROM ams.automator
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_all_automators_with_api_key():
    try:
        query = """
            SELECT id, name, brand, api_key
            FROM ams.automator
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_automators_by_ids(automator_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Get automator details for multiple automators including API keys.
    
    Args:
        automator_ids: List of automator UUIDs
        
    Returns:
        List of dictionaries with automator details
    """
    try:
        query = """
            SELECT 
                id::varchar,
                name,
                brand,
                api_key
            FROM ams.automator
            WHERE id = ANY(:automator_ids)
            ORDER BY name ASC
        """
        results = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"automator_ids": automator_ids}
        )
        return [dict(row) for row in results]
    except Exception as e:
        print(f"Error fetching automators: {str(e)}")
        return []


async def get_all_point_of_sale():
    try:
        query = """
            SELECT id, name
            FROM ams.point_of_sale
            ORDER BY name ASC
            """
        return await get_pg_readonly_database().fetch_all(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_account_automators(account_id: str, automator_ids: list[str], db=None):
    """
    Update account automator mappings.
    
    Args:
        account_id: The account ID to update
        automator_ids: List of automator IDs to associate
        db: Optional database connection. If provided, uses it directly (for participating
            in an existing transaction). If not provided, creates its own transaction.
    """
    try:
        owns_transaction = db is None
        if owns_transaction:
            db = get_pg_database()
        
        async def do_update():
            # First, get the brands of the automators we want to add
            if automator_ids:
                get_brands_query = """
                    SELECT id::varchar, brand
                    FROM ams.automator
                    WHERE id = ANY(:automator_ids)
                """
                new_automators = await db.fetch_all(
                    query=get_brands_query,
                    values={"automator_ids": automator_ids}
                )
                
                # Check for duplicate brands in the input
                brands_in_input = [a["brand"] for a in new_automators]
                if len(brands_in_input) != len(set(brands_in_input)):
                    raise HTTPException(
                        status_code=400,
                        detail="Cannot assign multiple automators with the same brand to one account"
                    )
                
                # Delete existing mappings for this account
                delete_query = """
                    DELETE FROM ams.account_automator_mapping 
                    WHERE account_id = :account_id
                    """
                await db.execute(query=delete_query, values={"account_id": account_id})
                
                # Insert new mappings with ON CONFLICT to handle any race conditions
                insert_query = """
                    INSERT INTO ams.account_automator_mapping (id, account_id, automator_id, brand)
                    SELECT uuid_generate_v4(), :account_id, a.id, a.brand
                    FROM ams.automator a
                    WHERE a.id = :automator_id
                    ON CONFLICT (account_id, brand) DO UPDATE
                    SET automator_id = EXCLUDED.automator_id
                    """
                values = [
                    {"account_id": account_id, "automator_id": automator_id}
                    for automator_id in automator_ids
                ]
                await db.execute_many(query=insert_query, values=values)
            else:
                # If no automator_ids provided, delete all mappings
                delete_query = """
                    DELETE FROM ams.account_automator_mapping 
                    WHERE account_id = :account_id
                    """
                await db.execute(query=delete_query, values={"account_id": account_id})

        if owns_transaction:
            async with db.transaction():
                await do_update()
        else:
            await do_update()

        return {"message": "Account automators updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating account automators for account {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_account_point_of_sale(account_id: str, pos_ids: list[str], db=None):
    """
    Update account point of sale mappings.
    
    Args:
        account_id: The account ID to update
        pos_ids: List of point of sale IDs to associate
        db: Optional database connection. If provided, uses it directly (for participating
            in an existing transaction). If not provided, creates its own transaction.
    """
    try:
        owns_transaction = db is None
        if owns_transaction:
            db = get_pg_database()
        
        async def do_update():
            delete_query = """
                DELETE FROM ams.account_point_of_sale_mapping 
                WHERE account_id = :account_id
                """
            await db.execute(query=delete_query, values={"account_id": account_id})

            if pos_ids:
                insert_query = """
                    INSERT INTO ams.account_point_of_sale_mapping (id, account_id, point_of_sale_id)
                    VALUES (uuid_generate_v4(), :account_id, :point_of_sale_id)
                    """
                values = [
                    {"account_id": account_id, "point_of_sale_id": pos_id}
                    for pos_id in pos_ids
                ]
                await db.execute_many(query=insert_query, values=values)

        if owns_transaction:
            async with db.transaction():
                await do_update()
        else:
            await do_update()

        return {"message": "Account point of sale updated successfully"}
    except Exception as e:
        logger.error(f"Error updating point of sale for account {account_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_proxy_star(proxy_id: str):
    try:
        query = """
        UPDATE ams.ams_proxy
        SET
            is_starred = NOT is_starred
        WHERE id = :proxy_id
        RETURNING id;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"proxy_id": proxy_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_proxies(proxies: list[ProxyRequestModel]):
    try:
        insert_query = """
            INSERT INTO ams.ams_proxy (
                id, proxy, zone, provider_id, proxy_metro, status_code, status_id, notes
            )
            VALUES (
                uuid_generate_v4(), :proxy, :zone, :provider_id, :proxy_metro, :status_code, (select id from ams.ams_proxy_status where code = :status_code), :notes
            )
            RETURNING id;
        """

        created_proxies = []
        for proxy in proxies:
            result = await get_pg_database().fetch_one(
                query=insert_query,
                values=proxy.model_dump(exclude={"ams_account_id"})  # proxy tableda account kerak emas
            )
            created_proxies.append(result)

            if proxy.ams_account_id:
                update_query = """
                    UPDATE ams.ams_account
                    SET ams_proxy_id = :proxy_id,
                    last_modified = CURRENT_TIMESTAMP
                    WHERE id = :account_id
                """
                await get_pg_database().execute(
                    query=update_query,
                    values={
                        "proxy_id": result["id"],
                        "account_id": proxy.ams_account_id
                    }
                )

        return {
            "inserted_count": len(created_proxies),
            "message": "Proxies created successfully."
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_proxy(proxy_id: str, new_proxy: ProxyRequestModel):
    db = get_pg_database()
    try:
        async with db.transaction():
            update_query = """
                UPDATE ams.ams_proxy ap
                SET 
                    proxy = :proxy,
                    zone = :zone,
                    provider_id = :provider_id,
                    proxy_metro = :proxy_metro,
                    status_code = :status_code,
                    status_id   = s.id,
                    notes = :notes,
                    last_modified = CURRENT_TIMESTAMP
                FROM ams.ams_proxy_status s
                WHERE s.code = :status_code
                  AND ap.id = :proxy_id
                RETURNING ap.id;
            """
            values = {
                **new_proxy.model_dump(exclude={"ams_account_id"}),
                "proxy_id": proxy_id,
            }
            updated_proxy = await db.fetch_one(query=update_query, values=values)

            old_acc = await db.fetch_one(
                "SELECT id FROM ams.ams_account WHERE ams_proxy_id = :proxy_id",
                {"proxy_id": proxy_id}
            )
            new_acc_id = new_proxy.ams_account_id

            # Case 1: no old account, new account provided → assign proxy to new account
            if not old_acc and new_acc_id:
                await db.execute(
                    """
                    UPDATE ams.ams_account
                    SET ams_proxy_id = :proxy_id,
                        last_modified = CURRENT_TIMESTAMP
                    WHERE id = :account_id
                    """,
                    {"proxy_id": proxy_id, "account_id": new_acc_id}
                )

            # Case 2: old account exists, new account is null → unassign proxy
            elif old_acc and not new_acc_id:
                await db.execute(
                    """
                    UPDATE ams.ams_account
                    SET ams_proxy_id = NULL,
                        last_modified = CURRENT_TIMESTAMP
                    WHERE id = :account_id
                    """,
                    {"account_id": old_acc["id"]}
                )

            # Case 3: old account exists, new account provided and different → reassign proxy
            elif old_acc and new_acc_id and str(old_acc["id"]) != str(new_acc_id):
                # Unassign proxy from old account
                await db.execute(
                    """
                    UPDATE ams.ams_account
                    SET ams_proxy_id = NULL,
                        last_modified = CURRENT_TIMESTAMP
                    WHERE id = :account_id
                    """,
                    {"account_id": old_acc["id"]}
                )
                # Assign proxy to new account
                await db.execute(
                    """
                    UPDATE ams.ams_account
                    SET ams_proxy_id = :proxy_id,
                        last_modified = CURRENT_TIMESTAMP
                    WHERE id = :account_id
                    """,
                    {"proxy_id": proxy_id, "account_id": new_acc_id}
                )

        return {"message": "Proxy updated successfully", "proxy_id": updated_proxy["id"]}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_proxy(proxy_id: str):
    try:
        query = """
            SELECT 
                    p.id, p.proxy, p.zone, p.status_code,
                    p.last_modified AT TIME ZONE 'UTC' AT TIME ZONE :timezone AS last_modified,
                    p.is_starred, p.notes,
                    jsonb_build_object(
                        'id', ma.id,
                        'name', ma.name,
                        'country', c.name,
                        'state', ms.abbreviation,
                        'timezone', jsonb_build_object(
                            'id', t.id,
                            'name', t.name,
                            'abbreviation', t.abbreviation
                        )
                    ) AS proxy_metro,
                    jsonb_build_object(
                        'id', pd.id,
                        'name', pd.name
                    ) AS provider,
                    jsonb_build_object(
                        'id', acc.id,
                        'nickname', acc.nickname
                    ) AS account
                FROM ams.ams_proxy AS p
                JOIN ams.ams_proxy_provider AS pd ON p.provider_id = pd.id
                LEFT JOIN ams.metro_area AS ma ON p.proxy_metro = ma.id
                LEFT JOIN ams.country AS c ON ma.country_id = c.id
                LEFT JOIN ams.state AS ms ON ma.state_id = ms.id
                LEFT JOIN ams.timezone AS t ON ma.timezone = t.id::TEXT
                LEFT JOIN ams.ams_account AS acc ON acc.ams_proxy_id = p.id
                WHERE  p.id = :proxy_id
            """
        result = await get_pg_readonly_database().fetch_one(
            query=query, values={"proxy_id": proxy_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_proxies_status(update_data: dict[str, list[str] | str]):
    try:
        proxy_ids = update_data['proxy_ids']

        # Create a placeholder for each ID
        placeholders = ', '.join([f':id{i}' for i in range(len(proxy_ids))])

        query = f"""
        UPDATE ams.ams_proxy
        SET
            status_code = :status,
            last_modified = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
        RETURNING id;
        """

        # Create values dict with individual ID parameters
        values = {"status": update_data['status']}
        for i, pid in enumerate(proxy_ids):
            values[f"id{i}"] = pid

        result = await get_pg_database().fetch_all(
            query=query,
            values=values
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def sync_vaultwarden_accounts(account_ids: List[str]):
    try:
        query = """
                SELECT 
                    a.id,
                    p.first_name, 
                    p.last_name, 
                    e.email_address, 
                    e.password 
                FROM ams.ams_account a
                JOIN ams.ams_email e ON e.id = a.ams_email_id
                JOIN ams.ams_person p ON p.id = a.ams_person_id
                AND a.id = ANY(:account_ids)
            """
        rows = await get_pg_readonly_database().fetch_all(
            query=query, values={"account_ids": account_ids}
        )

        vw_client = VaultwardenRegistration(environ["VAULTWARDEN_URL"])

        if not rows:
            return

        sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

        async def register_one(row):
            async with sem:
                full_name = f"{row['first_name']} {row['last_name']}".strip()
                email = row['email_address']
                password = row['password']

                if not row['password']:
                    return {
                        "account_id": row["id"],
                        "email": email,
                        "success": "Failed",
                        "result": "Password doesn't exist",
                    }

                success, result = await asyncio.to_thread(
                    vw_client.register_account,
                    email=email,
                    name=full_name,
                    master_password=password,
                    password_hint="",
                    kdf_iterations=600000,
                    captcha_response=None
                )
                return {
                    "account_id": row["id"],
                    "email": email,
                    "success": success,
                    "result": result,
                }

        tasks = [register_one(row) for row in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


def parse_proxy(proxy_str):
    try:
        host, port, username, password = proxy_str.strip().split(':')
        return {
            'Host': host,
            'Port': int(port),
            'Username': username,
            'Password': password
        }
    except ValueError:
        raise ValueError("Proxy string is not in expected format: host:port:username:password")


async def update_ticketsuite_persona_ids(successful_updates: List[Dict[str, Any]]) -> None:
    """
    Save persona IDs to primary_account_automator_mapping table.
    
    Args:
        successful_updates: List of dicts with keys:
            - persona_id: The persona ID returned by the automator
            - email_id: Email ID from primary_account
            - primary_id: Primary ID from primary_account
            - automator_id: The automator ID this persona belongs to (REQUIRED)
    """
    if not successful_updates:
        return

    try:
        db = get_pg_database()

        for update in successful_updates:
            automator_id = update.get('automator_id')
            if not automator_id:
                print(f"Warning: No automator_id provided for persona update, skipping")
                continue
                
            get_pa_id_query = """
                SELECT id FROM ams.primary_account
                WHERE email_id = :email_id AND primary_id = :primary_id
            """
            pa_result = await db.fetch_one(
                query=get_pa_id_query,
                values={
                    "email_id": update['email_id'],
                    "primary_id": update['primary_id']
                }
            )
            
            if pa_result:
                primary_account_id = pa_result['id']
                
                upsert_mapping_query = """
                    INSERT INTO ams.primary_account_automator_mapping 
                        (primary_account_id, automator_id, ams_automator_id)
                    VALUES 
                        (:primary_account_id, :persona_id, :automator_id)
                    ON CONFLICT (primary_account_id, ams_automator_id)
                    DO UPDATE SET 
                        ams_automator_id = EXCLUDED.ams_automator_id,
                        last_modified = CURRENT_TIMESTAMP
                """
                
                await db.execute(
                    query=upsert_mapping_query,
                    values={
                        "primary_account_id": str(primary_account_id),
                        "automator_id": automator_id,
                        "persona_id": update['persona_id']
                    }
                )
        
        print(f"Successfully saved {len(successful_updates)} persona IDs to mapping table")
    except Exception as e:
        print(f"Error saving persona IDs: {str(e)}")


async def get_persona_ids_by_automator(
    primary_account_id: str
) -> List[Dict[str, Any]]:
    """
    Get all automator-specific persona IDs for a given primary account.
    
    Args:
        primary_account_id: UUID of the primary_account record
        
    Returns:
        List of dicts with keys:
            - automator_id: UUID of the automator
            - automator_name: Name of the automator (e.g., "TicketSuite US", "Taciyon EU")
            - ams_automator_id: The persona/customer ID in that automator system
            - created_at: When this persona was created
            - updated_at: When this persona was last updated
    
    Example:
        [
            {
                "automator_id": "abc-123",
                "automator_name": "TicketSuite US",
                "ams_automator_id": "ts-persona-456",
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z"
            }
        ]
    """
    try:
        query = """
            SELECT 
                paam.automator_id::varchar,
                a.name AS automator_name,
                paam.ams_automator_id,
                paam.created_at,
                paam.updated_at
            FROM ams.primary_account_automator_mapping paam
            LEFT JOIN ams.automator a ON a.id = paam.automator_id
            WHERE paam.primary_account_id = :primary_account_id
            ORDER BY a.name ASC
        """
        
        rows = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"primary_account_id": primary_account_id}
        )
        
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"Error getting persona IDs by automator: {str(e)}")
        return []


async def get_all_persona_mappings_for_account(
    account_id: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get all persona mappings for an account, grouped by primary.
    This shows which personas exist in which automators for all primaries
    associated with this account.
    
    Args:
        account_id: Account ID (UUID)
        
    Returns:
        Dict mapping primary_name to list of persona mappings:
        {
            "Ticketmaster": [
                {
                    "automator_id": "abc-123",
                    "automator_name": "TicketSuite US",
                    "ams_automator_id": "ts-persona-456",
                    ...
                },
                ...
            ],
            "AXS": [...],
            ...
        }
    """
    try:
        query = """
            SELECT 
                pm.primary_name,
                pm.primary_code,
                pa.id::varchar AS primary_account_id,
                pa.primary_id::varchar,
                pa.email_id::varchar,
                paam.automator_id::varchar,
                a.name AS automator_name,
                paam.ams_automator_id,
                paam.created_at,
                paam.updated_at
            FROM ams.ams_account acc
            JOIN ams.primary_account pa ON pa.email_id = acc.ams_email_id
            JOIN ams.primary pm ON pm.id = pa.primary_id
            LEFT JOIN ams.primary_account_automator_mapping paam 
                ON paam.primary_account_id = pa.id
            LEFT JOIN ams.automator a ON a.id = paam.automator_id
            WHERE acc.id = :account_id
            ORDER BY pm.primary_name, a.name
        """
        
        rows = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"account_id": account_id}
        )

        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in rows:
            primary_name = row["primary_name"]
            if primary_name not in grouped:
                grouped[primary_name] = []
            
            if row["ams_automator_id"]:
                grouped[primary_name].append({
                    "primary_code": row["primary_code"],
                    "primary_account_id": row["primary_account_id"],
                    "primary_id": row["primary_id"],
                    "email_id": row["email_id"],
                    "automator_id": row["automator_id"],
                    "automator_name": row["automator_name"],
                    "ams_automator_id": row["ams_automator_id"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"]
                })
        
        return grouped
    except Exception as e:
        print(f"Error getting persona mappings for account: {str(e)}")
        return {}


async def validate_accounts_for_ts_sync(account_ids: List[str]) -> Dict[str, Any]:
    """
    Validate that accounts have required fields (company and automator) before syncing to TicketSuite.
    
    Args:
        account_ids: List of account IDs to validate
        
    Returns:
        Dictionary with validation results:
        {
            "valid_accounts": [account_ids],
            "invalid_accounts": [
                {
                    "account_id": "...",
                    "account_nickname": "...",
                    "missing_fields": ["company", "automator"]
                }
            ]
        }
    """
    query = """
        SELECT 
            a.id::varchar AS account_id,
            a.nickname AS account_nickname,
            a.company_id::varchar AS company_id,
            c.name AS company_name,
            COUNT(DISTINCT aam.automator_id) AS automator_count,
            ARRAY_AGG(DISTINCT am.id::varchar) FILTER (WHERE am.id IS NOT NULL) AS automator_ids,
            ARRAY_AGG(DISTINCT am.name) FILTER (WHERE am.name IS NOT NULL) AS automator_names
        FROM ams.ams_account a
        LEFT JOIN ams.company c ON c.id::varchar = a.company_id
        LEFT JOIN ams.account_automator_mapping aam ON aam.account_id = a.id
        LEFT JOIN ams.automator am ON am.id = aam.automator_id
        WHERE a.id = ANY(:account_ids)
        GROUP BY a.id, a.nickname, a.company_id, c.name
    """
    
    rows = await get_pg_readonly_database().fetch_all(
        query=query, values={"account_ids": account_ids}
    )
    
    valid_accounts = []
    invalid_accounts = []
    
    for row in rows:
        missing_fields = []
        
        # Check if company is filled
        if not row["company_id"]:
            missing_fields.append("company")
        
        # Check if at least one automator is assigned
        if row["automator_count"] == 0 or not row["automator_ids"]:
            missing_fields.append("automator")
        
        if missing_fields:
            invalid_accounts.append({
                "account_id": row["account_id"],
                "account_nickname": row["account_nickname"] or "Unnamed Account",
                "missing_fields": missing_fields
            })
        else:
            valid_accounts.append({
                "account_id": row["account_id"],
                "account_nickname": row["account_nickname"],
                "company_id": row["company_id"],
                "company_name": row["company_name"],
                "automator_ids": row["automator_ids"],
                "automator_names": row["automator_names"]
            })
    
    return {
        "valid_accounts": valid_accounts,
        "invalid_accounts": invalid_accounts
    }


async def get_accounts_with_primaries(account_ids: List[str]) -> List[AccountPrimary]:
    query = """
        SELECT 
            a.id::varchar AS account_id,
            a.nickname AS account_nickname,
            e.id::varchar AS email_id,
            e.email_address AS email_address, 
            pa.id::varchar AS primary_account_id,
            pa.primary_id::varchar AS primary_id,
            pm.primary_name AS primary_name,
            pm.primary_code AS primary_code,
            pa.password AS account_password,
            pa.ts_persona_id AS ticketsuite_persona_id,
            (
                SELECT COUNT(*) > 0
                FROM ams.primary_account_automator_mapping paam
                WHERE paam.primary_account_id = pa.id
            ) AS has_automator_mappings,
            CASE 
                WHEN LOWER(c.name) ILIKE '%shadows%' THEN FALSE
                WHEN pm.primary_code IN ('tmmt', 'ammt') THEN
                    EXISTS(
                        SELECT 1 
                        FROM ams.account_tag_mapping atm
                        JOIN ams.ams_account_tag tag ON tag.id = atm.tag_id
                        WHERE atm.account_id = a.id AND LOWER(tag.name) = 'juiced'
                    )
                ELSE FALSE
            END AS is_juiced,
            CASE 
                WHEN LOWER(c.name) ILIKE '%shadows%' THEN TRUE
                ELSE FALSE
            END AS is_shadows,
            p.proxy AS proxy,
            ph.number AS phone_number,
            php.label AS phone_provider,
            php.type AS phone_provider_type
        FROM ams.ams_account a
        LEFT JOIN ams.ams_email e ON e.id = a.ams_email_id
        LEFT JOIN ams.primary_account pa ON pa.email_id = e.id
        LEFT JOIN ams.primary pm ON pm.id = pa.primary_id
        LEFT JOIN ams.ams_proxy p ON p.id = a.ams_proxy_id
        LEFT JOIN ams.phone_number ph ON ph.id = a.phone_number_id
        LEFT JOIN ams.ams_phone_provider php ON ph.provider_id = php.id
        LEFT JOIN ams.company c ON c.id::varchar = a.company_id
        WHERE a.id = ANY(:account_ids)
        ORDER BY pm.primary_name ASC
    """
    rows = await get_pg_readonly_database().fetch_all(
        query=query, values={"account_ids": account_ids}
    )

    grouped = {}
    for row in rows:
        acc_id = row["account_id"]
        if acc_id not in grouped:
            proxy = None
            if row["proxy"]:
                try:
                    proxy = parse_proxy(row["proxy"])
                except ValueError:
                    proxy = None

            grouped[acc_id] = {
                "account_id": acc_id,
                "account_nickname": row["account_nickname"],
                "email_id": row["email_id"],
                "email_address": row["email_address"],
                "is_shadows": row["is_shadows"],
                "proxy": proxy,
                "phone": {
                    "PhoneNumber": row["phone_number"],
                    "Provider": row["phone_provider"],
                    "ProviderType": row["phone_provider_type"],
                    "IsRotation": True,
                    "IsEnabled": True
                },
                "primaries": []
            }

        if not row["primary_name"]:
            continue

        # Check if already added to TicketSuite and identify missing fields
        # Use has_automator_mappings to check if primary has been synced to any automator
        added_to_ts = bool(row["has_automator_mappings"])
        missing_fields = []

        if not added_to_ts:
            required_fields = [
                ("email_address", "Email address"),
                ("account_password", "Password"),
                ("primary_name", "Primary name"),
                ("primary_code", "Primary code")
            ]

            for field_key, field_name in required_fields:
                if not row[field_key]:
                    missing_fields.append(field_name)

        primary = Primary(
            id=row["primary_id"],
            primary_name=row["primary_name"],
            primary_code=row["primary_code"],
            password=row["account_password"],
            ticketsuite_persona_id=row["ticketsuite_persona_id"],
            is_juiced=row["is_juiced"],
            added_to_ts=added_to_ts,
            missing_fields=missing_fields
        )

        grouped[acc_id]["primaries"].append(primary)

    return [AccountPrimary(**account_data) for account_data in grouped.values()]


async def get_accounts_pos(account_ids: list[str]) -> dict[str, list[dict]]:
    try:
        query = """
            SELECT
                a.id,
                pos.id as pos_id,
                pos.name as pos_name
            FROM ams.ams_account a
            LEFT JOIN ams.account_point_of_sale_mapping aposm ON aposm.account_id = a.id
            LEFT JOIN ams.point_of_sale pos ON pos.id = aposm.point_of_sale_id
            WHERE a.id = ANY(:account_ids)
            """
        rows = await get_pg_readonly_database().fetch_all(
            query=query, values={"account_ids": account_ids}
        )
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(row["id"], [])
            if row["pos_id"]:
                grouped[row["id"]].append({
                    "id": str(row["pos_id"]),
                    "name": row["pos_name"]
                })

        return grouped
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_accounts_automators(account_ids: List[str]) -> dict[str, List[dict]]:
    try:
        query = """
            SELECT
                a.id,
                am.id as automator_id,
                am.name as automator_name,
                am.brand
            FROM ams.ams_account a
            LEFT JOIN ams.account_automator_mapping aam ON aam.account_id = a.id
            LEFT JOIN ams.automator am ON am.id = aam.automator_id
            WHERE a.id = ANY(:account_ids)
            """
        rows = await get_pg_readonly_database().fetch_all(
            query=query, values={"account_ids": account_ids}
        )
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(row["id"], [])
            if row["automator_id"]:
                grouped[row["id"]].append({
                    "id": str(row["automator_id"]),
                    "name": row["automator_name"],
                    "brand": row["brand"]
                })

        return grouped
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_mlx_account_creation_data(account_ids: List[str]) -> List[dict]:
    try:
        query = """
            SELECT
                  aa.id
                , aa.nickname
                , split_part(ap.proxy, ':', 1) AS proxy_host
                , split_part(ap.proxy, ':', 2) AS proxy_port
                , split_part(ap.proxy, ':', 3) AS proxy_user
                , split_part(ap.proxy, ':', 4) AS proxy_pass
                , ae.email_address
                , COALESCE(array_agg(aat.name ORDER BY aat.name), ARRAY[]::text[]) AS tags
            FROM
                ams.ams_account aa
            LEFT JOIN ams.ams_proxy ap ON
                ap.id = aa.ams_proxy_id
            LEFT JOIN ams.ams_email ae ON
                ae.id = aa.ams_email_id
            LEFT JOIN ams.account_tag_mapping atm ON
                atm.account_id = aa.id
            LEFT JOIN ams.ams_account_tag aat ON
                aat.id = atm.tag_id
            WHERE
                aa.id = ANY(CAST(:account_ids AS uuid[]))
            GROUP BY
                  aa.id
                , aa.nickname
                , ap.proxy
                , ae.email_address;
        """

        result = await get_pg_database().fetch_all(
            query=query, values={"account_ids": account_ids}
        )
        return [dict(row) for row in result]
    except Exception as e:
        print(f"Error fetching MLX account creation data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_accounts_with_mlx_ids(mlx_updates: List[dict]) -> List[dict]:
    try:
        query = """
            UPDATE
                ams.ams_account aa
            SET
                multilogin_id = v.multilogin_id
            FROM
                (
                    SELECT
                          UNNEST(:account_ids ::uuid[]) AS account_id
                        , UNNEST(:multilogin_ids ::uuid[]) AS multilogin_id
                ) AS v
            WHERE
                aa.id = v.account_id
            RETURNING
                  aa.id
                , aa.multilogin_id;
        """

        result = await get_pg_database().fetch_all(
            query=query,
            values={
                "account_ids": [update["id"] for update in mlx_updates],
                "multilogin_ids": [update["multilogin_id"] for update in mlx_updates],
            },
        )
        return [dict(row) for row in result]
    except Exception as e:
        print(f"Error updating MLX IDs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


def format_phone_number(phone: int | str) -> str:
        """
        Converts an integer or string phone number like 12694371155
        into formatted string like +1(269) 437-1155.
        Works for US-style numbers with optional country code.
        """
        digits = ''.join(filter(str.isdigit, str(phone)))

        if len(digits) == 11:
            country = f"+{digits[0]}"
            digits = digits[1:]
        elif len(digits) == 10:
            country = "+1"
        else:
            raise ValueError(f"Invalid phone number format: {phone}")

        area, first3, last4 = digits[:3], digits[3:6], digits[6:]
        return f"{country}({area}) {first3}-{last4}"


async def create_primary_account_mapping(
    account_id: str,
    primary_id: str,
    password: Optional[str] = None,
    generate_password: bool = False
) -> dict[str, Any]:
    """
    Adds a primary to an account.
    Args:
        password: The password for the primary.
        account_id: The ID of the account to add the primary to.
        primary_id: The ID of the primary to add to the account.
        password: The password for the primary.
        generate_password: whether to generate a password for the primary.
    Returns:
        A dictionary containing the result of the operation.
        The dictionary contains the following keys:
        - message: A message indicating the result of the operation.
        - result: The result of the operation.
        The result is a dictionary containing the following keys:
        - email_id: The ID of the email.
        - primary_id: The ID of the primary.
        - created_at: The timestamp of the creation of the primary.
    Raises:
        HTTPException: If password already exists (when provided by client) or if max retries exceeded.
    """
    db = get_pg_database()
    password_generator = PasswordGenerator()
    
    if generate_password:
        password = password_generator.generate()
    
    query = """
        INSERT INTO ams.primary_account (email_id, primary_id, password)
        SELECT a.ams_email_id, :primary_id, :password
        FROM ams.ams_account a
        WHERE a.id = :account_id
        RETURNING email_id, primary_id, created_at;
    """
    
    attempt = 0
    max_attempts = 10 if generate_password else 1
    
    while attempt < max_attempts:
        try:
            result = await db.execute(
                query=query, 
                values={"account_id": account_id, "primary_id": primary_id, "password": password}
            )
            return {"message": "Primary added to account successfully.", "result": result}
        except UniqueViolationError as e:
            error_message = str(e)
            if "password" in error_message.lower() or "uq_" in error_message.lower() or "unique" in error_message.lower():
                if generate_password:
                    attempt += 1
                    if attempt < max_attempts:
                        password = password_generator.generate()
                        continue
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Failed to generate a unique password after {max_attempts} attempts. Please try again."
                        )
                else:
                    raise HTTPException(
                        status_code=409,
                        detail="The provided password already exists in the database. Passwords must be unique. Please choose a different password."
                    )
            else:
                raise HTTPException(
                    status_code=409,
                    detail=f"Unique constraint violation: {error_message}"
                ) from e
                
    # This should never be reached, but satisfies linter
    raise HTTPException(
        status_code=500,
        detail="Unexpected error in create_primary_account_mapping"
    )


async def get_primary_ids_by_code(codes: list[str]) -> dict[str, str]:
    query = """
        SELECT id::varchar, primary_code FROM ams.primary WHERE primary_code = ANY(:codes)
    """
    rows = await get_pg_readonly_database().fetch_all(query=query, values={"codes": codes})
    return {row["primary_code"]: row["id"] for row in rows}


async def update_primary_account_mapping(
    account_id: str, # 96131d57-7228-42e7-aba7-417cc6905d3a
    primary_id: str, # "3ea5e2a7-6a68-4e84-9a57-149587a90542"
    password: str | None = None
) -> dict[str, Any]:
    """
    Updates a primary account mapping and corresponding TicketSuite persona password
    across ALL automators where personas were created for this primary.
    
    This function:
    1. Retrieves the primary_account record based on account_id and primary_id
    2. Updates the password in the primary_account table
    3. Queries primary_account_automator_mapping to find all automators with personas for this primary
    4. For each automator with a persona, updates the persona password in that TicketSuite instance
    
    Args:
        account_id: The ID of the account to update.
        primary_id: The ID of the primary to update (e.g., Ticketmaster, AXS).
        password: The new password for the primary account.
        
    Returns:
        A dictionary containing:
        - message: Success message
        - result: Database update result (email_id, primary_id, last_modified)
        - ticketsuite_updates: Results of TicketSuite updates per automator
            - successful: List of successfully updated automators with persona_id
            - failed: List of failed updates with error messages
            - skipped: List of skipped updates with reasons
        
    Raises:
        HTTPException: If the database update fails, primary mapping not found, or password constraint violation.
    """
    db = get_pg_database()
    
    get_info_query = """
        SELECT 
            pa.id::varchar as primary_account_id,
            pa.ts_persona_id,
            pa.email_id,
            e.email_address,
            a.id::varchar as account_id
        FROM ams.primary_account pa
        JOIN ams.ams_email e ON e.id = pa.email_id
        JOIN ams.ams_account a ON a.ams_email_id = pa.email_id
        WHERE a.id::varchar = :account_id
        AND pa.primary_id::varchar = :primary_id
    """
    persona_info = await db.fetch_one(
        query=get_info_query,
        values={"account_id": account_id, "primary_id": primary_id}
    )
    
    if not persona_info:
        raise HTTPException(
            status_code=404,
            detail=f"Primary account mapping not found for account_id={account_id}, primary_id={primary_id}"
        )
    
    primary_account_id = persona_info["primary_account_id"]
    account_email = persona_info["email_address"] if persona_info else None
    
    # Get all automators where personas were created for this primary
    persona_mappings_query = """
        SELECT 
            paam.automator_id::varchar,
            paam.ams_automator_id,
            a.name AS automator_name,
            a.api_key
        FROM ams.primary_account_automator_mapping paam
        JOIN ams.automator a ON a.id = paam.ams_automator_id
        WHERE paam.primary_account_id::varchar = :primary_account_id
    """
    persona_mappings = await db.fetch_all(
        query=persona_mappings_query,
        values={"primary_account_id": primary_account_id}
    )
    
    async with db.transaction():
        update_query = """
            UPDATE ams.primary_account 
            SET password = :password
            WHERE email_id = (
                SELECT ams_email_id 
                FROM ams.ams_account 
                WHERE id::varchar = :account_id
            )
            AND primary_id::varchar = :primary_id
            RETURNING email_id, primary_id, last_modified
        """
        try:
            result = await db.fetch_one(
                query=update_query,
                values={"account_id": account_id, "primary_id": primary_id, "password": password}
            )
        except UniqueViolationError as e:
            error_message = str(e)
            if "password" in error_message.lower() or "uq_" in error_message.lower() or "unique" in error_message.lower():
                raise HTTPException(
                    status_code=409,
                    detail="The provided password already exists in the database. Passwords must be unique. Please choose a different password."
                ) from e
            else:
                raise HTTPException(
                    status_code=409,
                    detail=f"Unique constraint violation: {error_message}"
                ) from e
    
    ts_update_results = {
        "successful": [],
        "failed": [],
        "skipped": []
    }
    
    if account_email and persona_mappings:
        print(f"Updating password in {len(persona_mappings)} TicketSuite instance(s) where persona exists...")
        for mapping in persona_mappings:
            ams_automator_id = mapping["ams_automator_id"]
            automator_name = mapping["automator_name"]
            persona_id = mapping["automator_id"]
            api_key = mapping["api_key"] if mapping["api_key"] else None
            
            try:
                if not api_key:
                    error_msg = f"No API key configured for automator {automator_name}"
                    print(f"Warning: {error_msg}")
                    ts_update_results["failed"].append({
                        "automator_id": ams_automator_id,
                        "automator_name": automator_name,
                        "persona_id": persona_id,
                        "error": error_msg
                    })
                    continue
                
                ts_service = get_ticketsuite_persona_client(api_key=api_key)
                
                async with ts_service:
                    # Get the existing persona data
                    persona_data = await ts_service.get(persona_id=persona_id)
                    if not persona_data:
                        error_msg = f"Persona {persona_id} not found in TicketSuite instance"
                        print(f"Warning: {error_msg} for automator {automator_name}")
                        ts_update_results["failed"].append({
                            "automator_id": ams_automator_id,
                            "automator_name": automator_name,
                            "persona_id": persona_id,
                            "error": error_msg
                        })
                        continue
                    
                    # Update the password
                    updated_persona = persona_data[0].model_copy(
                        update={"Password": password}
                    )

                    await ts_service.update(
                        persona_id=persona_id,
                        persona_payload=updated_persona
                    )
                    
                    ts_update_results["successful"].append({
                        "automator_id": ams_automator_id,
                        "automator_name": automator_name,
                        "persona_id": persona_id
                    })
                    print(f"✓ Updated password in TicketSuite for automator {automator_name} (persona_id: {persona_id})")
                    
            except Exception as e:
                error_msg = f"Error updating TicketSuite: {str(e)}"
                print(f"Error for automator {automator_name}: {error_msg}")
                ts_update_results["failed"].append({
                    "automator_id": ams_automator_id,
                    "automator_name": automator_name,
                    "persona_id": persona_id,
                    "error": error_msg
                })
    elif account_email and not persona_mappings:
        ts_update_results["skipped"].append({
            "reason": "No personas found in any automator for this primary account"
        })
        print("Warning: Account has email but no personas created in any automator - TicketSuite update skipped")
    else:
        ts_update_results["skipped"].append({
            "reason": "No email address found for account"
        })
        print("Warning: No email address found - TicketSuite update skipped")
        
    return {
        "message": "Primary password updated successfully.",
        "result": result,
        "ticketsuite_updates": ts_update_results
    }


async def get_searched_primaries(search_query: str):
    query = """
        SELECT 
            id,
            primary_name
        FROM ams.primary
        WHERE primary_name ILIKE :search_query
    """
    result = await get_pg_readonly_database().fetch_all(query=query, values={"search_query": f"%{search_query}%"})
    return result


async def create_primary(request: dict):
    query = """
        INSERT INTO ams.primary (
            id,
            primary_name,
            url
        )
        VALUES (
            :id,
            :primary_name,
            :url
        )
        RETURNING id, primary_name, url
    """

    values = {
        "id": str(uuid.uuid4()),
        "primary_name": request["primary_name"],
        "url": request.get("url")
    }

    result = await get_pg_database().fetch_one(query=query, values=values)
    return result


async def get_stages_steps(link: str | None = None):
    try:
        query = """
            SELECT json_agg(t ORDER BY t.stage_order)
            FROM (
              SELECT
              s.id,
              s.name,
              s.order_index AS stage_order,
              s.created_at,
              s.updated_at,
              s.stage_link,
              COALESCE(
            json_agg(
              json_build_object(
                'id', st.id,
                'stage_id', st.stage_id,
                'name', st.name,
                'type', st.type,
                'order_index', st.order_index,
                'api_details', st.api_details,
                'created_at', st.created_at,
                'updated_at', st.updated_at
              )
              ORDER BY st.order_index
            ) FILTER (WHERE st.id IS NOT NULL),
            '[]'::json
              ) AS steps
            FROM ams.stages s
            LEFT JOIN ams.steps st
              ON st.stage_id = s.id
            {where_clause}
            GROUP BY
              s.id, s.name, s.order_index, s.created_at, s.updated_at
            ) AS t;
        """
        where_clause = ""
        values = {}
        if link:
            where_clause = "WHERE s.stage_link = :link"
            values["link"] = link
        query = query.format(where_clause=where_clause)
        result = await get_pg_readonly_database().fetch_all(query=query, values=values)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_stages(stage_name: str, stage_order: int, stage_link: str | None):
    try:
        query = """
            INSERT INTO ams.stages (name, order_index, stage_link)
            VALUES (:stage_name, :stage_order, :stage_link)
            RETURNING id, name, order_index, stage_link;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"stage_name": stage_name, "stage_order": stage_order, "stage_link": stage_link}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_stage_order(stage_id: str, new_order_index: int):
    """
    Updates the order_index of a stage by atomically swapping its order with
    the stage currently at the target order_index.
    """
    try:
        query = """
            UPDATE ams.stages
            SET order_index = :new_order_index
            WHERE id = :stage_id
        """

        result = await get_pg_database().execute(query, {
            "new_order_index": new_order_index,
            "stage_id": stage_id
        })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_stage_link(stage_id: str, new_stage_link: str | None):
    """
    Updates the stage_link of a stage.
    """
    try:
        query = """
            UPDATE ams.stages
            SET stage_link = :new_stage_link,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :stage_id
            RETURNING id, name, order_index, stage_link, created_at, updated_at;
        """

        result = await get_pg_database().fetch_one(query, {
            "new_stage_link": new_stage_link,
            "stage_id": stage_id
        })

        if not result:
            raise ValueError("Stage not found.")

        return result

    except ValueError as ve:
        raise ve
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def delete_stage(stage_id: str):
    """
    Deletes a stage and all its associated steps.
    The database is configured with ON DELETE CASCADE, so associated steps
    will be automatically deleted when the stage is deleted.
    """
    try:
        db = get_pg_database()

        # Use a transaction to ensure the delete succeeds or fails completely
        async with db.transaction():
            # Delete the stage - associated steps will be automatically deleted
            # due to ON DELETE CASCADE constraint
            delete_stage_query = """
                DELETE FROM ams.stages
                WHERE id = :stage_id
            """
            result = await db.execute(delete_stage_query, {"stage_id": stage_id})

            if result == 0:
                raise ValueError("Stage not found.")

        return {"message": "Stage deleted successfully."}

    except Exception as e:
        # The transaction will automatically be rolled back on any exception
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_single_stage_with_steps(stage_id: str):
    """
    Retrieves a single stage with all its associated steps.
    """
    try:
        query = """
            SELECT
                s.id,
                s.name,
                s.order_index,
                s.created_at,
                s.updated_at,
                COALESCE(
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', st.id,
                            'name', st.name,
                            'type', st.type,
                            'order_index', st.order_index,
                            'api_details', st.api_details,
                            'created_at', st.created_at,
                            'updated_at', st.updated_at
                        ) ORDER BY st.order_index ASC
                    ) FILTER (WHERE st.id IS NOT NULL),
                    '[]'::json
                ) AS steps
            FROM ams.stages s
            LEFT JOIN ams.steps st ON s.id = st.stage_id
            WHERE s.id = :stage_id
            GROUP BY s.id, s.name, s.order_index, s.created_at, s.updated_at;
        """

        result = await get_pg_readonly_database().fetch_one(
            query=query,
            values={"stage_id": stage_id}
        )

        if not result:
            raise ValueError("Stage not found.")

        return dict(result)

    except ValueError as ve:
        raise ve
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_stage_name(stage_id: str, new_name: str):
    """
    Updates a stage's name and updated_at timestamp.
    """
    try:
        query = """
            UPDATE ams.stages
            SET 
                name = :new_name,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = :stage_id
            RETURNING id, name, order_index, created_at, updated_at;
        """

        result = await get_pg_database().fetch_one(
            query=query,
            values={"stage_id": stage_id, "new_name": new_name}
        )

        if not result:
            raise ValueError("Stage not found.")

        return result

    except ValueError as ve:
        raise ve
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_step(stage_id: str, step_data: CreateStep):
    try:
        query = """
            INSERT INTO ams.steps (stage_id, name, type, order_index, api_details)
            VALUES (:stage_id, :name, :type, :order_index, :api_details)
            RETURNING (id, name, type, order_index, api_details);
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={"stage_id": stage_id, "name": step_data.name, "type": step_data.type,
                    "order_index": step_data.order_index, "api_details": step_data.api_details}
        )
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_step_order(step_id: str, stage_id: str, new_order_index: int):
    """
    Updates the order_index of a stage by atomically swapping its order with
    the stage currently at the target order_index.
    """
    try:
        query = """
            UPDATE ams.steps
            SET order_index = :new_order_index
            WHERE id = :step_id
        """

        result = await get_pg_database().execute(query, {
            "new_order_index": new_order_index,
            "step_id": step_id
        })
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def delete_step(step_id: str):
    """
    Deletes a step by its ID.
    """
    try:
        db = get_pg_database()

        # Use a transaction to ensure the delete succeeds or fails completely
        async with db.transaction():
            # Delete the step
            delete_step_query = """
                DELETE FROM ams.steps
                WHERE id = :step_id
            """
            result = await db.execute(delete_step_query, {"step_id": step_id})

            if result == 0:
                raise ValueError("Step not found.")

        return {"message": "Step deleted successfully."}

    except Exception as e:
        # The transaction will automatically be rolled back on any exception
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_step_dependencies(step_id: str):
    """
    Gets all dependencies for a specific step.
    """
    try:
        query = """
            SELECT 
                sd.prerequisite_step_id,
                s.name as prerequisite_step_name,
                s.stage_id
            FROM ams.step_dependencies sd
            JOIN ams.steps s ON sd.prerequisite_step_id = s.id
            WHERE sd.dependent_step_id = :step_id
            ORDER BY s.order_index
        """
        result = await get_pg_database().fetch_all(query, {"step_id": step_id})
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_step(step_id: str, step_name: str, step_type: str, api_details: str = None):
    """
    Updates a step's basic information.
    """
    try:
        query = """
            UPDATE ams.steps
            SET name = :step_name, type = :step_type, api_details = :api_details, updated_at = CURRENT_TIMESTAMP
            WHERE id = :step_id
            RETURNING id, stage_id, name, type, api_details, order_index, created_at, updated_at
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={
                "step_id": step_id,
                "step_name": step_name,
                "step_type": step_type,
                "api_details": api_details
            }
        )

        if not result:
            raise ValueError("Step not found.")

        return result
    except ValueError as ve:
        raise HTTPException(status_code=404, detail=str(ve))
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_step_dependencies(step_id: str, prerequisite_step_ids: list[str]):
    """
    Updates the dependencies for a step by replacing all existing dependencies with the new ones.
    """
    try:
        db = get_pg_database()

        async with db.transaction():
            # Delete existing dependencies for this step
            delete_query = """
                DELETE FROM ams.step_dependencies
                WHERE dependent_step_id = :step_id
            """
            await db.execute(delete_query, {"step_id": step_id})

            # Insert new dependencies
            if prerequisite_step_ids:
                insert_query = """
                    INSERT INTO ams.step_dependencies (dependent_step_id, prerequisite_step_id)
                    VALUES (:dependent_step_id, :prerequisite_step_id)
                """
                for prerequisite_id in prerequisite_step_ids:
                    await db.execute(
                        insert_query,
                        {
                            "dependent_step_id": step_id,
                            "prerequisite_step_id": prerequisite_id
                        }
                    )

        return {"message": "Step dependencies updated successfully."}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_stages_steps_filter_data():
    """
    Gets all stages and steps for populating filter dropdowns in the frontend.
    """
    try:
        # Return an array of stage objects each with its steps (id, name) only.
        # Shape matches the frontend interface StageStepFilter:
        # [{ stage_id, stage_name, steps: [{ id, name }] }]
        query = """
            WITH stage_data AS (
                SELECT 
                    s.id AS stage_id,
                    s.name AS stage_name,
                    s.order_index,
                    COALESCE(
                        (
                            SELECT json_agg(json_build_object('id', st.id, 'name', st.name) ORDER BY st.order_index)
                            FROM ams.steps st
                            WHERE st.stage_id = s.id
                        ),
                        '[]'::json
                    ) AS steps
                FROM ams.stages s
            )
            SELECT COALESCE(
                json_agg(
                    json_build_object(
                        'id', stage_id,
                        'name', stage_name,
                        'steps', steps
                    ) ORDER BY order_index
                ), '[]'::json
            ) AS data
            FROM stage_data;
        """
        row = await get_pg_readonly_database().fetch_one(query)
        return row["data"] if row and row["data"] else []
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_steps_for_account(account_ids: list[str]):
    """
    Gets all steps associated with a specific account.
    """
    try:
        query = """
        WITH ids AS (SELECT unnest(CAST(:account_ids AS uuid[])) AS account_id)
        SELECT json_build_object(
          'stages', COALESCE((SELECT json_agg(to_jsonb(s) ORDER BY s.order_index) FROM ams.stages s), '[]'::json),
          'steps',  COALESCE((SELECT json_agg(to_jsonb(st) ORDER BY st.order_index) FROM ams.steps st), '[]'::json),
          'completed_steps', COALESCE((
            SELECT json_agg(
                     json_build_object(
                       'account_id', x.account_id,
                       'steps', COALESCE(x.steps, '[]'::json)
                     )
                     ORDER BY x.account_id
                   )
            FROM (
              SELECT
                i.account_id,
                json_agg(
                  json_build_object(
                    'id', acs.id,
                    'account_id', acs.account_id,
                    'step_id', acs.step_id,
                    'time_completed', acs.time_completed
                  )
                  ORDER BY acs.time_completed
                ) AS steps
              FROM ids i
              LEFT JOIN ams.account_completed_steps acs
                ON acs.account_id = i.account_id
              GROUP BY i.account_id
            ) x
          ), '[]'::json)
        ) AS data;
        """

        rows = await get_pg_readonly_database().fetch_all(
            query=query,
            values={"account_ids": account_ids},  # list[str] of UUIDs
        )
        return [row["data"] for row in rows] if rows else [{"stages": [], "steps": [], "completed_steps": []}]
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def complete_steps_for_account(account_id: str, steps: list[str]):
    """
    Marks multiple steps as completed for a specific account.
    """
    try:
        query = """
            INSERT INTO ams.account_completed_steps (account_id, step_id)
            VALUES (:account_id, :step_id)
            ON CONFLICT (account_id, step_id) DO NOTHING;
        """
        values = [{"account_id": account_id, "step_id": step_id} for step_id in steps]
        await get_pg_database().execute_many(query=query, values=values)
        return {"message": f"Marked {len(steps)} steps as completed for account {account_id}."}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def uncomplete_step_for_account(account_id: str, steps: list[str]):
    """
    Marks multiple steps as uncompleted for a specific account.
    """
    try:
        query = """
            DELETE FROM ams.account_completed_steps
            WHERE account_id = :account_id AND step_id = :step_id
        """
        values = [{"account_id": account_id, "step_id": step_id} for step_id in steps]
        await get_pg_database().execute_many(query=query, values=values)
        return {"message": f"Unmarked {len(steps)} steps as completed for account {account_id}."}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_steps_for_dependencies(stage_id: str, current_step_id: str):
    """
    Gets all steps in a stage that can be used as dependencies, excluding the current step.
    """
    try:
        query = """
            SELECT id, name, order_index
            FROM ams.steps
            WHERE stage_id = :stage_id AND id != :current_step_id
            ORDER BY order_index
        """
        result = await get_pg_database().fetch_all(
            query=query,
            values={"stage_id": stage_id, "current_step_id": current_step_id}
        )
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_dependent_steps(stage_id: str, current_step_id: str):
    """
    Gets all steps in a stage that depend on the current step (to prevent circular dependencies).
    """
    try:
        query = """
            SELECT s.id, s.name, s.order_index
            FROM ams.steps s
            JOIN ams.step_dependencies sd ON s.id = sd.dependent_step_id
            WHERE sd.prerequisite_step_id = :current_step_id
            AND s.stage_id = :stage_id
            ORDER BY s.order_index
        """
        result = await get_pg_database().fetch_all(
            query=query,
            values={"stage_id": stage_id, "current_step_id": current_step_id}
        )
        return result
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_ams_account_multilogin_id(account_id: str, ml_id: str):
    """
    Updates the multilogin ID for a specific AMS account.
    """
    try:
        query = """
            UPDATE ams.ams_account
            SET multilogin_id = :ml_id
            WHERE id = :account_id
        """
        await get_pg_database().execute(
            query=query, values={"account_id": account_id, "ml_id": ml_id}
        )
        print(f"Updated AMS account {account_id} multilogin ID to {ml_id}.")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_email_two_fa(email_id: str) -> list[EmailTwoFAResponseModel]:
    """
    Get all 2FA methods for a specific email.
    """
    try:
        query = """
            SELECT id::varchar, ams_email_id::varchar as email_id, type, value, active
            FROM ams.ams_email_2fa
            WHERE ams_email_id = :email_id
            ORDER BY created_at ASC
        """
        result = await get_pg_readonly_database().fetch_all(
            query=query, values={"email_id": email_id}
        )
        return [EmailTwoFAResponseModel(**dict(row)) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_email_two_fa(email_id: str, two_fa_data: list[EmailTwoFARequestModel]) -> list[EmailTwoFAResponseModel]:
    """
    Update 2FA methods for a specific email.
    This function replaces all existing 2FA methods with the new ones.
    """
    try:
        active_count = sum(1 for item in two_fa_data if item.active)
        if active_count > 1:
            raise HTTPException(
                status_code=400,
                detail="Only one 2FA method can be active at a time. Please ensure only one method has 'active' set to true."
            )

        async with get_pg_database().transaction():
            delete_query = """
                DELETE FROM ams.ams_email_2fa
                WHERE ams_email_id = :email_id
            """
            await get_pg_database().execute(
                query=delete_query, values={"email_id": email_id}
            )

            if two_fa_data:
                insert_query = """
                    INSERT INTO ams.ams_email_2fa (id, ams_email_id, type, value, active, created_at, last_modified)
                    VALUES (:id, :email_id, :type, :value, :active, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
                values = []
                for item in two_fa_data:
                    values.append({
                        "id": uuid.uuid4(),
                        "email_id": email_id,
                        "type": item.type,
                        "value": item.value,
                        "active": item.active
                    })

                await get_pg_database().execute_many(query=insert_query, values=values)

            return await get_email_two_fa(email_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_proxy_change_reasons():
    try:
        query = """
            SELECT id, code, label FROM ams.ams_proxy_change_reason ORDER BY label
            """
        result = await get_pg_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_proxy_and_reason(account_id: str, request: dict):
    try:
        db = get_pg_database()
        async with db.transaction():
            # First get current proxy ID before updating
            current_proxy_query = """
                SELECT ams_proxy_id FROM ams.ams_account WHERE id = :account_id
            """
            current_proxy_result = await db.fetch_one(
                query=current_proxy_query,
                values={"account_id": account_id}
            )

            current_proxy_id = current_proxy_result["ams_proxy_id"] if current_proxy_result else None
            new_proxy_id = request["ams_proxy_id"]

            # Get the Persona Account from Ticket Suite
            email_request = request["email"]
            automators = request["automators"]
            persona_update_result = None

            if isinstance(automators, list):
                automator_list = automators if automators else []
                automator_str = ", ".join(automator_list) if automator_list else ""
            else:
                automator_list = [automators] if automators else []
                automator_str = automators or ""

            automator_query = """
                SELECT name FROM ams.automator 
                WHERE id = ANY(:automator_ids)
                ORDER BY name
            """

            automator_result = await db.fetch_one(
                query=automator_query,
                values={"automator_ids": automator_list}
            )

            automator_name = automator_result['name'] if automator_result else "None"

             # Sync proxy changes to corresponding TicketSuite personas
            if automator_result and automator_result['name'] != 'None':
                #if automator_str:
                for automator in automator_list:
                    if email_request:
                        persona_update_result = await _update_persona_proxy(email_request, new_proxy_id, automator, automator_name)
            else:
                persona_update_result = (
                    f"\nNo proxy sync to TicketSuite for persona {email_request}\n "
                    "because no automator is selected."
                )

            # Update account's proxy and reason
            query = """
                UPDATE ams.ams_account 
                SET 
                    ams_proxy_id = :ams_proxy_id,
                    last_modified = CURRENT_TIMESTAMP,
                    updated_by = :updated_by
                WHERE id = :account_id
                RETURNING ams_proxy_id
            """
            result = await db.fetch_one(
                query=query,
                values={"account_id": account_id, "ams_proxy_id": new_proxy_id, "updated_by": request["updated_by"]}
            )

            # Update proxy statuses
            if current_proxy_id:
                # Set old proxy as AVAILABLE
                await update_proxies_status({
                    'proxy_ids': [current_proxy_id],
                    'status': 'REPLACED'
                })

            if new_proxy_id:
                # Set new proxy as IN_USE
                await update_proxies_status({
                    'proxy_ids': [new_proxy_id],
                    'status': 'IN_USE'
                })

            # Create change log
            await create_proxy_change_log({
                "profile_name": request["account_nickname"],
                "replaced_by_proxy_id": new_proxy_id, # ID of the new proxy
                "reason": request["ams_change_reason_id"],
                "replaced_by": request["updated_by"],
                "proxy_id": current_proxy_id # ID of the old proxy being replaced
            })

            return {"message": "Proxy and change reason updated successfully.", "result": result, "persona_update_result": persona_update_result}

    except HTTPException:
        # Let HTTPExceptions (e.g. 404 for missing persona) bubble up unchanged
        raise
    except Exception as e:
        # Wrap unexpected errors as 500
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

async def create_proxy_change_log(update_data: dict[str, list[str] | str]):
    try:
        query = """
            INSERT INTO ams.ams_proxy_replaced_history (
                id, 
                profile_name, 
                date_replaced, 
                replaced_by_proxy_id, 
                reason, 
                replaced_by, 
                proxy_id)
            VALUES (
                uuid_generate_v4(), 
                :profile_name, 
                CURRENT_TIMESTAMP, 
                :replaced_by_proxy_id, 
                :reason, 
                :replaced_by, 
                :proxy_id
            )
            RETURNING *;
        """
        result = await get_pg_database().fetch_one(
            query=query,
            values={
                "profile_name": update_data["profile_name"],
                "replaced_by_proxy_id": update_data["replaced_by_proxy_id"],
                "reason": update_data["reason"],
                "replaced_by": update_data["replaced_by"],
                "proxy_id": update_data["proxy_id"]
            }
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

async def _update_persona_proxy(email_request: list[dict], proxy_id: str, automator_id: str, automator_name: Optional[str] = None):
    try:
        # determine which TicketSuite API key to use based on company and point_of_sale
        
        credentials = await ts_credential_manager.get_credentials_for_automator(UUID(automator_id))
        if not credentials:
            error_msg = f"No TicketSuite credentials found for automator {automator_name}"
            raise HTTPException(
                status_code=404,
                detail=error_msg
            )
        else:
            ts_service = get_ticketsuite_persona_client(api_key=credentials.api_key)

        async with ts_service:
            persona_data = await ts_service.get(email=str(email_request))

            # collect all "id" values from persona_data
            persona_ids = [p.Id for p in persona_data if p.Id is not None]

            if not persona_data:
                raise HTTPException(
                    status_code=404,
                    detail=(f"\nPersona {email_request} was not found in {automator_name}.")
                )
            
            # If we have persona ids and a new_proxy_id, fetch proxy string and patch each persona
            ts_update_results = {"success": [], "failures": []}
            if persona_ids and proxy_id:
                # fetch proxy string from DB
                proxy_row = await get_pg_readonly_database().fetch_one(
                    query="SELECT proxy FROM ams.ams_proxy WHERE id = :id",
                    values={"id": proxy_id}
                )
                proxy_str = proxy_row["proxy"] if proxy_row else None
                if proxy_str:
                    try:
                        proxy_dict = parse_proxy(proxy_str)
                        proxy_model = TsProxyPayload(**proxy_dict)
                    except Exception:
                        proxy_model = None

                    if proxy_model:
                        for pid in persona_ids:
                            try:
                                resp = await ts_service.update_proxy(persona_id=str(pid), proxy_payload=proxy_model)
                                ts_update_results["success"].append({
                                    "persona_id": pid,
                                    "response": resp.model_dump(),
                                    "message": f"Proxy sync complete for {email_request} at {automator_name}."
                                })
                            except HTTPException as he:
                                ts_update_results["failures"].append({"persona_id": pid, "status": getattr(he, "status_code", None), "detail": he.detail if hasattr(he, "detail") else str(he)})
                            except Exception as err:
                                ts_update_results["failures"].append({"persona_id": pid, "error": str(err)})
                else:
                    ts_update_results["failures"].append({"error": f"Proxy record not found for id {proxy_id}"})
            return ts_update_results
    except Exception as e:
        print(f"Error updating persona proxy for profiles: {e}")
        raise


async def sync_ticketsuite_proxy(request: dict):
    try:
        db = get_pg_database()
        async with db.transaction():
            
            new_proxy_id = request["ams_proxy_id"]

            # Get the Persona Account from Ticket Suite
            email_request = request["email"]
            automators = request["automators"]
            persona_update_result = None

            if isinstance(automators, list):
                automator_list = automators if automators else []
            else:
                automator_list = [automators] if automators else []

            automator_query = """
                SELECT name FROM ams.automator 
                WHERE id = ANY(:automator_ids)
                ORDER BY name
            """

            automator_result = await db.fetch_one(
                query=automator_query,
                values={"automator_ids": automator_list}
            )

            automator_name = automator_result['name'] if automator_result else "None"

             # Sync proxy changes to corresponding TicketSuite personas
            if automator_result and automator_result['name'] != 'None':
                #if automator_str:
                for automator in automator_list:
                    if email_request:
                        persona_update_result = await _update_persona_proxy(email_request, new_proxy_id, automator, automator_name)
            else:
                
                persona_update_result = (
                    f"\nNo proxy sync to TicketSuite for persona {email_request}\n "
                    f"because no automator is selected."
                )

            return { "message": persona_update_result }

    except HTTPException:
        # Let HTTPExceptions (e.g. 404 for missing persona) bubble up unchanged
        raise
    except Exception as e:
        # Wrap unexpected errors as 500
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
