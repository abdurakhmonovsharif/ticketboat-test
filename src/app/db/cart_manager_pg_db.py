import asyncio
import csv
import json
import os
import time
from datetime import datetime, timedelta
from io import StringIO
from typing import Optional, List, Dict

import boto3
import httpx
from fastapi import HTTPException

from app.database import get_pg_database
from app.model.cart_manager import CartManagerUpdateNew, BulkCartManagerUpdate
from app.model.user import User

sqs_client = boto3.client('sqs')
queue_url = os.getenv('SQS_UPDATE_CART_STATUS_QUEUE_URL')

browser_capture_api_url = os.getenv('BROWSER_CAPTURE_API_URL')

async def get_all_carts(
        status: Optional[str] = None,
        event_codes: Optional[List[str]] = None,
        time_status: Optional[List[str]] = None,
        captain_list: Optional[List[str]] = None,
        page: int = None,
        page_size: int = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        timezone: str = "America/Chicago",
        view: str = "cart",
        company: Optional[str] = None,
        on_sale_event_code: Optional[str] = None,
        on_sale_link: Optional[str] = None,
        on_sale_venue: Optional[str] = None,
        on_sale_city: Optional[str] = None,
        event_date_time_timestamp: Optional[str] = None,
        search_term: Optional[str] = None,
):
    current_time = int(time.time()) * 1000

    base_query = """
        FROM browser_data_capture.cart c
        LEFT JOIN email.ticketboat_user_email tue ON c.multilogin_profile = tue.nickname
    """

    conditions = ["1=1"]
    values = {}

    if status:
        conditions.append("c.status = :status")
        values['status'] = status

    if event_codes:
        conditions.append("c.event_name = ANY(:event_codes)")
        values['event_codes'] = event_codes

    if event_date_time_timestamp:
        event_date_time_timestamp_obj = datetime.strptime(event_date_time_timestamp, "%Y-%m-%dT%H:%M:%S")
        conditions.append("(c.event_date_time_iso = CAST(:event_date_time_timestamp AS timestamp))")
        values['event_date_time_timestamp'] = event_date_time_timestamp_obj

    if on_sale_event_code or on_sale_link or on_sale_venue or on_sale_city:
        like_conditions = []

        if on_sale_event_code:
            like_conditions.append("c.event_code ILIKE :on_sale_event_code")
            values['on_sale_event_code'] = f"%{on_sale_event_code}%"

        if on_sale_link:
            like_conditions.append("c.event_url ILIKE :on_sale_link")
            values['on_sale_link'] = f"%{on_sale_link}%"

        if on_sale_venue:
            like_conditions.append("LOWER(c.venue) ILIKE LOWER(:on_sale_venue)")
            values['on_sale_venue'] = f"%{on_sale_venue}%"

        if on_sale_city:
            like_conditions.append("LOWER(c.venue) ILIKE LOWER(:on_sale_city)")
            values['on_sale_city'] = f"%{on_sale_city}%"

        conditions.append("(" + " OR ".join(like_conditions) + ")")

    if time_status:
        time_conditions = []
        for status in time_status:
            if status == 'expired':
                time_conditions.append(f"(c.hold_ends_at IS NOT NULL AND c.hold_ends_at < {current_time})")
            elif status == 'never_expire':
                time_conditions.append("c.hold_ends_at IS NULL")
            elif status == 'unexpired':
                time_conditions.append(f"(c.hold_ends_at IS NOT NULL AND c.hold_ends_at > {current_time})")

        if time_conditions:
            conditions.append("(" + " OR ".join(time_conditions) + ")")

    if captain_list:
        conditions.append("c.captain_email = ANY(:captain_list)")
        values['captain_list'] = captain_list

    if company and company.lower() != "all":
        company_list = [s.strip() for s in company.split(",")]
        conditions.append("tue.company = ANY(:companies)")
        values["companies"] = company_list

    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        conditions.append(
            "DATE_TRUNC('day', TO_TIMESTAMP(c.created) AT TIME ZONE :timezone) >= DATE_TRUNC('day', CAST(:start_date AS timestamp))"
        )
        values['start_date'] = start_date_obj
        values['timezone'] = timezone

    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        conditions.append("DATE_TRUNC('day', TO_TIMESTAMP(c.created) AT TIME ZONE :timezone) <= DATE_TRUNC('day', CAST(:end_date AS timestamp))")
        values['end_date'] = end_date_obj
        values['timezone'] = timezone

    if search_term:
        search_conditions = [
            "LOWER(c.event_name) LIKE LOWER(:search_term)",
            "LOWER(c.venue) LIKE LOWER(:search_term)",
            "LOWER(c.event_date_time) LIKE LOWER(:search_term)"
        ]
        conditions.append("(" + " OR ".join(search_conditions) + ")")
        values['search_term'] = f"%{search_term}%"

    where_clause = " WHERE " + " AND ".join(conditions)

    # Run widget and main queries concurrently
    db = get_pg_database()
    widgets_task = asyncio.create_task(get_widgets_metrics(base_query, where_clause, values, current_time))

    if view == "show":
        main_query, count_query = await get_show_view_query(base_query, where_clause, page, page_size)
    elif view == "tour":
        main_query, count_query = await get_tour_view_query(base_query, where_clause, page, page_size)
    elif view == "tour_and_show":
        main_query, count_query = await get_tour_and_show_view_query(base_query, where_clause, page, page_size)
    else:
        main_query, count_query = await get_cart_view_query(base_query, where_clause, page, page_size)

    main_query_task = asyncio.create_task(db.fetch_all(main_query, values))
    count_query_task = asyncio.create_task(db.fetch_val(count_query, values))

    # Wait for results
    widgets = await widgets_task
    results = await main_query_task
    total_count = await count_query_task

    # Process results
    processed_results = []
    for row in results:
        row_dict = dict(row)
        if view == "cart":
            row_dict['seats'] = parse_json_field(row_dict.get('seats'))
            row_dict['face_value_per_ticket'] = parse_json_field(row_dict.get('face_value_per_ticket'))
        processed_results.append(row_dict)

    return {
        "items": processed_results,
        "total": total_count,
        "widgets": widgets
    }


def parse_json_field(field):
    if field is None:
        return []
    try:
        loads = json.loads(field)
        return loads
    except json.JSONDecodeError:
        return field


async def get_cart_view_query(
        base_query: str,
        where_clause: str,
        page: int = None,
        page_size: int = None
):
    main_query = f"""
            WITH filtered_carts AS (
                SELECT 
                    c.*,
                    tue.company as company
                {base_query}
                {where_clause}
            ),
            seat_totals AS (
                SELECT 
                    cart_id, 
                    SUM(quantity) as total_seats
                FROM browser_data_capture.seat s
                WHERE s.cart_id IN (SELECT id FROM filtered_carts)
                GROUP BY cart_id
            ),
            valid_carts AS (
                SELECT fc.*
                FROM filtered_carts fc
                JOIN seat_totals st ON fc.id = st.cart_id
                WHERE st.total_seats > 0
                ORDER BY fc.created DESC
                {f"LIMIT {page_size} OFFSET {(page - 1) * page_size}" if page is not None and page_size is not None else ""}
            ),
            face_values AS (
                SELECT 
                    cart_id,
                    jsonb_agg(face_value_row) as face_value_data
                FROM browser_data_capture.face_value_per_ticket
                WHERE cart_id IN (SELECT id FROM valid_carts)
                GROUP BY cart_id
            )

            SELECT 
                vc.id, vc.marketplace, 
                vc.created as created,
                TO_CHAR(
                    (TO_TIMESTAMP(vc.created) AT TIME ZONE :timezone),
                    'YYYY-MM-DD"T"HH24:MI:SS.US'
                ) as created_str,
                vc.comment, vc.email,
                vc.event_date_time_timestamp,
                vc.event_name, vc.event_code, vc.venue, vc.event_date_time, vc.hold_ends_at,
                vc.status, vc.seating_info, vc.total_fees, vc.total_cost, vc.seat_map_url,
                vc.event_url, vc.order_processing_fee, vc.currency_symbol, vc.pt_version,
                vc.status_updated_by, vc.status_updated_at, vc.captain_email, vc.multilogin_profile,
                vc.company,
                COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'section', s.section,
                            'row', s.row,
                            'seat', s.seat,
                            'quantity', s.quantity
                        )
                    ) FILTER (WHERE s.id IS NOT NULL),
                    '[]'::jsonb
                ) as seats,
                COALESCE(fv.face_value_data, '[]'::jsonb) as face_value_per_ticket
            FROM valid_carts vc
            LEFT JOIN browser_data_capture.seat s ON vc.id = s.cart_id
            LEFT JOIN face_values fv ON vc.id = fv.cart_id
            GROUP BY vc.id, vc.marketplace, vc.created, vc.comment, vc.email,
                     vc.event_name, vc.event_code, vc.venue, vc.event_date_time, vc.hold_ends_at,
                     vc.status, vc.seating_info, vc.total_fees, vc.total_cost, vc.seat_map_url,
                     vc.event_url, vc.order_processing_fee, vc.currency_symbol, vc.pt_version,
                     vc.status_updated_by, vc.status_updated_at, vc.captain_email, vc.multilogin_profile, 
                     vc.company, fv.face_value_data, vc.event_date_time_timestamp
            ORDER BY vc.created DESC
        """

    count_query = f"""
        WITH filtered_carts AS (
            SELECT c.id
            {base_query}
            {where_clause}
        ),
        seat_totals AS (
            SELECT 
                cart_id
            FROM browser_data_capture.seat s
            WHERE s.cart_id IN (SELECT id FROM filtered_carts)
            GROUP BY cart_id
            HAVING SUM(quantity) > 0
        )
        SELECT COUNT(*) as total
        FROM filtered_carts fc
        WHERE fc.id IN (SELECT cart_id FROM seat_totals)
    """

    return main_query, count_query


async def get_show_view_query(base_query: str, where_clause: str, page: int = None, page_size: int = None):
    main_query = f"""
        SELECT 
            MD5(CONCAT(c.event_name, c.venue, c.event_date_time)) AS id,
            c.event_name,
            c.venue,
            c.event_date_time,
            COUNT(DISTINCT c.id) AS cart_count,
            SUM(CASE WHEN c.status = 'approve' THEN 1 ELSE 0 END) AS approved_cart_count,
            SUM(CASE WHEN c.status = 'decline' THEN 1 ELSE 0 END) AS declined_cart_count,
            SUM(CASE WHEN c.status = 'approve' THEN c.total_cost ELSE 0 END) AS total_cost, 
            COUNT(DISTINCT CASE 
                WHEN c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000
                THEN c.email 
            END) AS buyer_count,
            COUNT(DISTINCT CASE 
                WHEN (
                    (c.hold_ends_at > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 OR
                    (c.hold_ends_at IS NULL AND c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000))
                    AND c.status = 'pending'
                )
                THEN c.id 
            END) AS pending_carts
        {base_query}
        {where_clause}
        GROUP BY c.event_name, c.venue, c.event_date_time
        ORDER BY pending_carts DESC, MIN(c.created) DESC
        {f"LIMIT {page_size} OFFSET {(page - 1) * page_size}" if page is not None and page_size is not None else "LIMIT 100"}
    """

    count_query = f"""
        SELECT COUNT(DISTINCT CONCAT(c.event_name, c.venue, c.event_date_time)) as total
        {base_query}
        {where_clause}
    """

    return main_query, count_query


async def get_tour_view_query(base_query: str, where_clause: str, page: int = None, page_size: int = None):
    main_query = f"""
        SELECT 
            MD5(c.event_name) AS id,
            c.event_name,
            COUNT(DISTINCT c.id) AS cart_count,
            SUM(CASE WHEN c.status = 'approve' THEN 1 ELSE 0 END) AS approved_cart_count,
            SUM(CASE WHEN c.status = 'decline' THEN 1 ELSE 0 END) AS declined_cart_count,
            SUM(CASE WHEN c.status = 'approve' THEN c.total_cost ELSE 0 END) AS total_cost,
            COUNT(DISTINCT CASE 
                WHEN c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000
                THEN c.email 
            END) AS buyer_count,
            COUNT(DISTINCT CASE 
                WHEN (
                    (c.hold_ends_at > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 OR
                    (c.hold_ends_at IS NULL AND c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000))
                    AND c.status = 'pending'
                )
                THEN c.id 
            END) AS pending_carts
        {base_query}
        {where_clause}
        GROUP BY c.event_name
        ORDER BY pending_carts DESC, MIN(c.created) DESC
        {f"LIMIT {page_size} OFFSET {(page - 1) * page_size}" if page is not None and page_size is not None else "LIMIT 100"}
    """

    count_query = f"""
        SELECT COUNT(DISTINCT c.event_name) as total
        {base_query}
        {where_clause}
    """

    return main_query, count_query


async def get_tour_and_show_view_query(base_query: str, where_clause: str, page: int = None, page_size: int = None):
    main_query = f"""
        WITH show_metrics AS (
            SELECT 
                c.event_name,
                c.venue,
                c.event_date_time,
                COUNT(DISTINCT c.id) AS cart_count,
                SUM(CASE WHEN c.status = 'approve' THEN 1 ELSE 0 END) AS approved_cart_count,
                SUM(CASE WHEN c.status = 'decline' THEN 1 ELSE 0 END) AS declined_cart_count,
                SUM(CASE WHEN c.status = 'approve' THEN c.total_cost ELSE 0 END) AS total_cost,
                COUNT(DISTINCT CASE 
                    WHEN c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000
                    THEN c.email 
                END) AS buyer_count,
                COUNT(DISTINCT CASE 
                    WHEN (
                        (c.hold_ends_at > EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 OR
                        (c.hold_ends_at IS NULL AND c.created >= EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - INTERVAL '20 minutes')) * 1000))
                        AND c.status = 'pending'
                    )
                    THEN c.id 
                END) AS pending_carts,
                MIN(c.created) as first_created
            {base_query}
            {where_clause}
            GROUP BY c.event_name, c.venue, c.event_date_time
        )
        SELECT 
            MD5(event_name) AS tour_id,
            event_name AS tour_name,
            JSONB_AGG(
                JSONB_BUILD_OBJECT(
                    'id', MD5(CONCAT(event_name, venue, event_date_time)),
                    'venue', venue,
                    'event_date_time', event_date_time,
                    'cart_count', cart_count,
                    'approved_cart_count', approved_cart_count,
                    'declined_cart_count', declined_cart_count,
                    'total_cost', total_cost,
                    'buyer_count', buyer_count,
                    'pending_carts', pending_carts,
                    'created', first_created
                ) ORDER BY first_created desc
            ) AS shows,
            SUM(cart_count) AS cart_count,
            SUM(approved_cart_count) AS approved_cart_count,
            SUM(declined_cart_count) AS declined_cart_count,
            SUM(total_cost) AS total_cost,
            SUM(buyer_count) AS buyer_count,
            SUM(pending_carts) AS pending_carts
        FROM show_metrics
        GROUP BY event_name
        ORDER BY pending_carts DESC, MIN(first_created) DESC
        {f"LIMIT {page_size} OFFSET {(page - 1) * page_size}" if page is not None and page_size is not None else "LIMIT 100"}
    """

    count_query = f"""
        SELECT COUNT(DISTINCT c.event_name) as total
        {base_query}
        {where_clause}
    """

    return main_query, count_query


async def get_specific_carts(
        id: str,
        view: str,
        status: Optional[str] = None,
        event_codes: Optional[List[str]] = None,
        time_status: Optional[List[str]] = None,
        captain_list: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        timezone: str = "America/Chicago"
):
    conditions = ["1=1"]
    values = {'id': id, 'timezone': timezone}

    if status:
        conditions.append("c.status = :status")
        values['status'] = status

    if event_codes:
        conditions.append("c.event_name = ANY(:event_codes)")
        values['event_codes'] = event_codes

    if time_status:
        current_time = int(time.time()) * 1000
        time_conditions = []
        for status in time_status:
            if status == 'expired':
                time_conditions.append(f"(c.hold_ends_at IS NOT NULL AND c.hold_ends_at < {current_time})")
            elif status == 'never_expire':
                time_conditions.append("c.hold_ends_at IS NULL")
            elif status == 'unexpired':
                time_conditions.append(f"(c.hold_ends_at IS NOT NULL AND c.hold_ends_at > {current_time})")

        if time_conditions:
            conditions.append("(" + " OR ".join(time_conditions) + ")")

    if captain_list:
        conditions.append("c.captain_email = ANY(:captain_list)")
        values['captain_list'] = captain_list

    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        conditions.append(
            "DATE_TRUNC('day', TO_TIMESTAMP(c.created) AT TIME ZONE :timezone) >= DATE_TRUNC('day', CAST(:start_date AS timestamp))"
        )
        values['start_date'] = start_date_obj

    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        conditions.append(
            "DATE_TRUNC('day', TO_TIMESTAMP(c.created) AT TIME ZONE :timezone) <= DATE_TRUNC('day', CAST(:end_date AS timestamp))"
        )
        values['end_date'] = end_date_obj

    where_clause = " AND " + " AND ".join(conditions)

    # CTE based on view type
    if view == "show":
        info_cte = """
        WITH target_info AS (
            SELECT 
                event_name,
                venue,
                event_date_time
            FROM browser_data_capture.cart
            GROUP BY event_name, venue, event_date_time
            HAVING MD5(CONCAT(event_name, venue, event_date_time)) = :id
        ),
        seat_totals AS (
            SELECT 
                cart_id, 
                SUM(quantity) as total_seats
            FROM browser_data_capture.seat
            GROUP BY cart_id
        ),
        face_values AS (
            SELECT 
                cart_id,
                jsonb_agg(face_value_row) as face_value_data
            FROM browser_data_capture.face_value_per_ticket
            GROUP BY cart_id
        )
        """
        join_condition = """
        JOIN target_info ti ON 
            c.event_name = ti.event_name AND 
            c.venue = ti.venue AND 
            c.event_date_time = ti.event_date_time
        """
    else:  # tour
        info_cte = """
        WITH target_info AS (
            SELECT 
                event_name
            FROM browser_data_capture.cart
            GROUP BY event_name
            HAVING MD5(event_name) = :id
        ),
        seat_totals AS (
            SELECT 
                cart_id, 
                SUM(quantity) as total_seats
            FROM browser_data_capture.seat
            GROUP BY cart_id
        ),
        face_values AS (
            SELECT 
                cart_id,
                jsonb_agg(face_value_row) as face_value_data
            FROM browser_data_capture.face_value_per_ticket
            GROUP BY cart_id
        )
        """
        join_condition = "JOIN target_info ti ON c.event_name = ti.event_name"

    query = f"""
    {info_cte}
    SELECT 
        c.*,
        TO_CHAR(
            (TO_TIMESTAMP(c.created_str, 'YYYY-MM-DD"T"HH24:MI:SS.US') AT TIME ZONE :timezone),
            'YYYY-MM-DD"T"HH24:MI:SS.US'
        ) as created_str,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'section', s.section,
                    'row', s.row,
                    'seat', s.seat,
                    'quantity', s.quantity
                )
            ) FILTER (WHERE s.id IS NOT NULL),
            '[]'::jsonb
        ) as seats,
        COALESCE(fv.face_value_data, '[]'::jsonb) as face_value_per_ticket
    FROM browser_data_capture.cart c
    {join_condition}
    LEFT JOIN browser_data_capture.seat s ON c.id = s.cart_id
    LEFT JOIN seat_totals st ON c.id = st.cart_id
    LEFT JOIN face_values fv ON c.id = fv.cart_id
    WHERE 1=1 {where_clause} AND st.total_seats > 0
    GROUP BY 
        c.id, c.marketplace, c.created, c.comment, c.email,
        c.event_name, c.event_code, c.venue, c.event_date_time, c.hold_ends_at,
        c.status, c.seating_info, c.total_fees, c.total_cost, c.seat_map_url,
        c.event_url, c.order_processing_fee, c.currency_symbol, c.pt_version,
        c.status_updated_by, c.status_updated_at, c.captain_email, c.multilogin_profile,
        c.created_str, fv.face_value_data
    ORDER BY c.created DESC
    """

    db = get_pg_database()
    results = await db.fetch_all(query, values)

    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"{'Show' if view == 'show' else 'Tour'} not found"
        )

    return {
        "items": [
            {
                **dict(row),
                'seats': parse_json_field(row['seats']),
                'face_value_per_ticket': parse_json_field(row['face_value_per_ticket'])
            }
            for row in results
        ]
    }


async def update_status(update_data: CartManagerUpdateNew, user: User):
    check_query = """
        SELECT status
        FROM browser_data_capture.cart
        WHERE id = :id
    """
    check_values = {"id": update_data.id}

    current_status = await get_pg_database().fetch_val(check_query, check_values)

    if current_status is None:
        raise HTTPException(status_code=404, detail="Cart not found")

    status_updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    update_query = """
        UPDATE browser_data_capture.cart
        SET status = :status,
            comment = COALESCE(:comment, comment),
            status_updated_by = :status_updated_by,
            status_updated_at = :status_updated_at
        WHERE id = :id
        RETURNING id, status, comment, status_updated_by, status_updated_at
    """
    update_values = {
        "id": update_data.id,
        "status": str.lower(update_data.status),
        "comment": update_data.comment,
        "status_updated_by": user.email,
        "status_updated_at": status_updated_at
    }

    result = await get_pg_database().fetch_one(update_query, update_values)
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{browser_capture_api_url}/notify-carts",
                json={"cart_ids": [update_data.id]}
            )
            if response.status_code == 200:
                print(f"Successfully notified browser capture API for cart: {update_data.id}")
            else:
                print(f"Failed to notify browser capture API for cart: {update_data.id}, status: {response.status_code}")
    except Exception as e:
        print(f"Error notifying browser capture API for cart {update_data.id}: {str(e)}")

    return {
        "id": result["id"],
        "status": result["status"],
        "comment": result["comment"],
        "status_updated_by": result["status_updated_by"],
        "status_updated_at": result["status_updated_at"]
    }


def convert_to_csv(data):
    output = StringIO()
    output.write('\ufeff')
    writer = csv.writer(output)

    writer.writerow([
        'Request Date & Time', 'User Email', 'Event Name', 'Venue',
        'Event Date & Time', 'Sec/Row/Seat', 'Total Quantity', 'Face Value Per Ticket', 'Total Cost',
        'Order Processing Fee',
        'Total Fees', 'Comment', 'Status', 'Status Updated By', 'Status Updated At'
    ])

    for item in data:
        writer.writerow([
            format_date_time(item.get('created_str', '')),
            item.get('email', ''),
            item.get('event_name', ''),
            item.get('venue', ''),
            item.get('event_date_time', ''),
            format_seating_info(item.get('seats', [])),
            calculate_total_seats_quantity(item.get('seats', [])),
            format_face_value_per_ticket_info(item.get('face_value_per_ticket', [])),
            item.get('total_cost', 0),
            item.get('order_processing_fee', 0),
            item.get('total_fees', 0),
            item.get('comment', ''),
            item.get('status', ''),
            item.get('status_updated_by', ''),
            item.get('status_updated_at', '')
        ])
    return output.getvalue()


def format_seating_info(seats):
    if not seats:
        return "NA / NA / NA"
    return " / ".join([
        f"{seat.get('section', 'NA')} / {seat.get('row', 'NA')} / {seat.get('seat', 'NA')}" for seat in seats
    ])


def format_face_value_per_ticket_info(face_value_tickets):
    if not face_value_tickets:
        return "NA / NA / NA"
    return " / ".join([str(face_value_ticket) for face_value_ticket in face_value_tickets])


def calculate_total_seats_quantity(seats):
    count = 0
    for seat in seats:
        count += int(seat.get('quantity', 0))
    return count


def format_date_time(date_time_str):
    if not date_time_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_time_str)
        return dt.strftime("%m/%d/%Y %H:%M:%S")
    except ValueError:
        return date_time_str


async def get_carts_events_title(hour: int) -> List[Dict[str, str]]:
    current_time = datetime.now()
    past_time = current_time - timedelta(hours=hour)
    past_time_timestamp = int(past_time.timestamp())

    query = """
        SELECT DISTINCT
            c.event_name as event_name,
            tue.company as company
        FROM browser_data_capture.cart c
        LEFT JOIN email.ticketboat_user_email tue ON c.multilogin_profile = tue.nickname
        WHERE created >= :past_time
        ORDER BY c.event_name, tue.company
    """

    db = get_pg_database()
    results = await db.fetch_all(query, {"past_time": past_time_timestamp})

    return [{"event_name": row["event_name"], "company": row["company"]} for row in results]


async def get_widgets_metrics(
        base_query: str,
        where_clause: str,
        values: dict,
        current_time: int
) -> dict:
    twenty_minutes_ago = current_time - (20 * 60)

    widgets_query = f"""
        WITH widgets_metrics AS (
            SELECT 
                 COUNT(DISTINCT CASE 
                    WHEN (
                        (c.hold_ends_at > {current_time} OR
                        (c.hold_ends_at IS NULL AND c.created >= {twenty_minutes_ago}))
                        AND c.status = 'pending'
                    )
                    THEN c.id 
                    END) as pending_carts,
                MIN(CASE 
                    WHEN c.hold_ends_at > {current_time}
                    THEN c.hold_ends_at
                    END) as shortest_time_left,
                COALESCE(SUM(CASE WHEN c.status = 'approve' THEN s.quantity ELSE 0 END), 0) as total_purchased_qty,
                COALESCE(SUM(CASE WHEN c.status = 'approve' THEN c.total_cost ELSE 0 END), 0) as total_amount_spent,
                 COUNT(DISTINCT CASE 
                    WHEN c.created >= {twenty_minutes_ago} 
                    THEN c.email 
                    END) as active_buyers
            {base_query}
            LEFT JOIN browser_data_capture.seat s ON c.id = s.cart_id
            {where_clause}
        )
        SELECT 
            pending_carts,
            shortest_time_left,
            total_purchased_qty,
            total_amount_spent,
            active_buyers
        FROM widgets_metrics
    """

    db = get_pg_database()
    result = await db.fetch_one(widgets_query, values)

    return {
        "pending_carts": result['pending_carts'] or 0,
        "shortest_time_left": result['shortest_time_left'] or "0:00",
        "total_purchased_qty": result['total_purchased_qty'] or 0,
        "total_amount_spent": result['total_amount_spent'] or 0,
        "active_buyers": result['active_buyers'] or 0
    }


async def bulk_update_status(update_data: BulkCartManagerUpdate, user: User):
    check_query = """
        SELECT id, status
        FROM browser_data_capture.cart
        WHERE id = ANY(:ids)
    """
    check_values = {"ids": update_data.cart_ids}

    db = get_pg_database()
    current_statuses = await db.fetch_all(check_query, check_values)

    found_ids = {row['id'] for row in current_statuses}
    missing_ids = set(update_data.cart_ids) - found_ids
    if missing_ids:
        raise HTTPException(
            status_code=404,
            detail=f"Carts not found: {', '.join(missing_ids)}"
        )

    non_pending = [
        row['id'] for row in current_statuses
        if row['status'] != 'pending'
    ]
    if non_pending:
        raise HTTPException(
            status_code=400,
            detail=f"Carts must be in pending status: {', '.join(non_pending)}"
        )

    status_updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    update_query = """
        UPDATE browser_data_capture.cart
        SET status = :status,
            comment = COALESCE(:comment, comment),
            status_updated_by = :status_updated_by,
            status_updated_at = :status_updated_at
        WHERE id = ANY(:ids)
        RETURNING id, status, comment, status_updated_by, status_updated_at
    """
    update_values = {
        "ids": update_data.cart_ids,
        "status": str.lower(update_data.status),
        "comment": update_data.comment,
        "status_updated_by": user.email,
        "status_updated_at": status_updated_at
    }

    results = await db.fetch_all(update_query, update_values)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{browser_capture_api_url}/notify-carts",
                json={"cart_ids": update_data.cart_ids}
            )
            if response.status_code == 200:
                print(f"Successfully notified browser capture API for {len(update_data.cart_ids)} carts")
            else:
                print(f"Failed to notify browser capture API, status: {response.status_code}")
    except Exception as e:
        print(f"Error notifying browser capture API for bulk update: {str(e)}")

    return {
        "updated_carts": [
            {
                "id": result["id"],
                "status": result["status"],
                "comment": result["comment"],
                "status_updated_by": result["status_updated_by"],
                "status_updated_at": result["status_updated_at"]
            }
            for result in results
        ],
        "total_updated": len(results)
    }
