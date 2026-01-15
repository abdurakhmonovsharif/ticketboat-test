import traceback
import snowflake.connector
from typing import List, Dict, Any
from fastapi import HTTPException
from app.database import get_pg_realtime_catalog_database, get_pg_database, get_snowflake_connection
from app.model.shadows_listings import *
from app.model.user import User


async def get_viagogo_listings() -> List[ShadowsViagogoListingsModel]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                select id, event_id, event_name, start_date, venue, city, state_province, country
                from viagogo_listings
                QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY start_date ASC) = 1
                LIMIT 50
            """)
            results = cur.fetchall()
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsViagogoListingsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_vivid_listings() -> List[ShadowsVividListingsModel]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                select id, production_id AS event_id, event_name, start_date, venue, city, state
                FROM vivid_listings
                QUALIFY ROW_NUMBER() OVER (PARTITION BY production_id ORDER BY start_date ASC) = 1
                LIMIT 50
            """)
            results = cur.fetchall()
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsVividListingsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_gotickets_listings() -> List[ShadowsGoTicketsListingsModel]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                select
                    gl.id AS id,
                    ge.id AS event_id,
                    ge.name AS event_name,
                    ge.EVENT_TIME_LOCAL AS start_date,
                    ge.VENUE_NAME AS venue,
                    ge.VENUE_CITY AS city,
                    ge.VENUE_STATE AS state_province,
                    ge.VENUE_COUNTRY AS country
                FROM GOTICKETS_LISTINGS gl
                LEFT JOIN GOTICKETS_EVENT ge
                ON gl.EVENT_ID = ge.ID 
                QUALIFY ROW_NUMBER() OVER (PARTITION BY ge.id ORDER BY ge.EVENT_TIME_LOCAL desc) = 1
                LIMIT 50
            """)
            results = cur.fetchall()
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsGoTicketsListingsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def get_seatgeek_listings() -> List[ShadowsSeatGeekListingsModel]:
    try:
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    sl.id,
                    sl.event_id,
                    sl.event AS "event_name",
                    CONCAT(sl.event_date, ' ', sl.event_time) AS start_date,
                    sl.venue,
                    sv.city AS city,
                    sv.state AS state_province,
                    sv.country AS country
                FROM seatgeek_listings sl
                LEFT JOIN seat_geek_events se ON se.id = sl.event_id
                LEFT JOIN seat_geek_venues sv ON sv.id = se.venue_id
                QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY start_date ASC) = 1
                LIMIT 50
            """)
            results = cur.fetchall()
            items = []
            for result in results:
                normalized_data = {key.lower(): value for key, value in result.items()} # type: ignore
                items.append(ShadowsSeatGeekListingsModel(**normalized_data))
            return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def retrieve_listing_db(id: str, market: str) -> Dict[str, Any]: # type: ignore
    try:
        if market == 'viagogo':
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    """
                        select
                            id,
                            event_id,
                            section,
                            "row",
                            ticket_price,
                            viagogo_account_id as account,
                            external_id as section_id
                        from viagogo_listings where event_id = %(event_id)s
                    """, {"event_id": id})
                results = cur.fetchall()
                items = [ViagogoListingModel(**{key.lower(): value for key, value in input_data.items()}) for input_data in results] # type: ignore
                return {"items": items, "total": len(results)}

        if market == 'vivid':
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    """
                        select
                            id,
                            production_id as event_id,
                            section,
                            "row",
                            price as ticket_price,
                            account,
                            ticket_id as section_id
                        from vivid_listings where production_id = %(production_id)s
                    """, {"production_id": id}
                )
                results = cur.fetchall()
                items = [VividListingsModel(**{key.lower(): value for key, value in input_data.items()}) for input_data in results] # type: ignore
                return {"items": items, "total": len(results)}
        if market == 'gotickets':
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    """
                        select
                            id,
                            event_id,
                            section,
                            "row",
                            price as ticket_price,
                            account,
                            external_ticket_id as section_id
                        from gotickets_listings where event_id = %(event_id)s
                    """, {"event_id": id}
                )
                results = cur.fetchall()
                items = [GoTicketsListingsModel(**{key.lower(): value for key, value in input_data.items()}) for input_data in results] # type: ignore
                return {"items": items, "total": len(results)}
        
        if market == 'seatgeek':
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(
                    """
                    select
                        sl.id,
                        sl.event_id,
                        sl.section,
                        sl."row",
                        sl.cost as ticket_price,
                        sl.account,
                        seim.external_id as section_id
                    from seatgeek_listings sl
                    left join seatgeek_external_id_map seim
                        on seim.sg_listing_id = sl.seller_listing_id
                    where sl.event_id = %(event_id)s
                    """, {"event_id": id}
                )
                results = cur.fetchall()
                items = [SeatGeekListingsModel(**{key.lower(): value for key, value in input_data.items()}) for input_data in results] # type: ignore
                return {"items": items, "total": len(results)}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

async def search_listing_db(params: ShadowsListingSearchModel) -> Dict[str, Any]:
    markets = params.market
    viagogo_items = []
    vivid_items = []
    gotickets_items = []
    seatgeek_items = []

    for market in markets: # type: ignore
        table = f"{market}_listings"
        sql = create_sql_query(params, table=table)
        print(sql)
        try:
            with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(sql)
                results = cur.fetchall()
                if results:
                    if market == 'viagogo':
                        viagogo_items.extend([
                            ShadowsViagogoListingsModel(**{key.lower(): value for key, value in input_data.items()})
                            for input_data in results
                        ])
                    elif market == 'vivid':
                        vivid_items.extend([
                            ShadowsVividListingsModel(**{key.lower(): value for key, value in input_data.items()})
                            for input_data in results
                        ])
                    elif market == 'gotickets':
                        gotickets_items.extend([
                            ShadowsGoTicketsListingsModel(**{key.lower(): value for key, value in input_data.items()})
                            for input_data in results
                        ])
                    elif market == 'seatgeek':
                        seatgeek_items.extend([
                            ShadowsSeatGeekListingsModel(**{key.lower(): value for key, value in input_data.items()})
                            for input_data in results
                        ])
        except Exception as e:
            traceback.print_exc()
            print('ERROR: ', str(e))
            raise HTTPException(status_code=500, detail=str(e))

    print(gotickets_items)

    return ShadowsListingsModel (
        viagogo=viagogo_items,
        vivid=vivid_items,
        gotickets=gotickets_items,
        seatgeek=seatgeek_items
    ).to_items_format()

def create_sql_query(data: ShadowsListingSearchModel, table: str) -> str:
    conditions = []
    is_gotickets = table == "gotickets_listings"
    is_seatgeek = table == "seatgeek_listings"

    for key, value in data.model_dump(exclude_none=True).items():
        if key == "start_date":
            if is_gotickets:
                field = "ge.EVENT_TIME_UTC"
            elif is_seatgeek:
                field = "CONCAT(sl.event_date, ' ', sl.event_time)"
            else:
                field = "start_date"
            conditions.append(f"CAST({field} AS TEXT) LIKE '%{value}%'")
        elif key != "market":
            if is_gotickets:
                if key in ["event_name", "venue"]:
                    field = {
                        "event_name": "ge.name",
                        "venue": "ge.VENUE_NAME"
                    }.get(key, f"ge.{key}")
                    conditions.append(f"{field} ILIKE '%{value}%'")
                else:
                    conditions.append(f"{key} ILIKE '%{value}%'")
            elif is_seatgeek:
                if key in ["event_name", "venue"]:
                    field = {
                        "event_name": "sl.event",
                        "venue": "sl.venue"
                    }.get(key, f"sl.{key}")
                    conditions.append(f"{field} ILIKE '%{value}%'")
                else:
                    conditions.append(f"sl.{key} ILIKE '%{value}%'")
            else:
                conditions.append(f"{key} ILIKE '%{value}%'")

    where_clause = " AND ".join(conditions)

    if is_gotickets:
        query = """
        SELECT
            cast(ge.id as string) AS id,
            gl.event_id AS event_id,
            ge.name AS event_name,
            ge.EVENT_TIME_LOCAL AS start_date,
            ge.VENUE_NAME AS venue,
            ge.VENUE_CITY AS city,
            ge.VENUE_STATE AS state_province,
            ge.VENUE_COUNTRY AS country
        FROM GOTICKETS_LISTINGS gl
        LEFT JOIN GOTICKETS_EVENT ge ON gl.EVENT_ID = ge.ID
        """
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " QUALIFY ROW_NUMBER() OVER (PARTITION BY ge.id ORDER BY start_date ASC) = 1 limit 200"

        return query

    elif is_seatgeek:
        query = """
        SELECT 
            sl.id,
            sl.event_id,
            sl.event AS "event_name",
            CONCAT(sl.event_date, ' ', sl.event_time) AS start_date,
            sl.venue,
            NULL AS city,
            NULL AS state_province,
            NULL AS country
        FROM seatgeek_listings sl
        """
        if where_clause:
            query += f" WHERE {where_clause}"
        query += " QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY start_date ASC) = 1 limit 200"
        return query

    else:
        # Default to viagogo or vivid
        viagogo_sql = f"SELECT * FROM {table} "
        vivid_sql = f"SELECT production_id AS event_id, * FROM {table} "
        query = viagogo_sql if table == "viagogo_listings" else vivid_sql

        if where_clause:
            if table == "viagogo_listings":
                query += f" WHERE {where_clause} AND event_id IS NOT NULL QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY start_date ASC) = 1"
            else:
                query += f" WHERE {where_clause} QUALIFY ROW_NUMBER() OVER (PARTITION BY production_id ORDER BY start_date ASC) = 1"

        return query
