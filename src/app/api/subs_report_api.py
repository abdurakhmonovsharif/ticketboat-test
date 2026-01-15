import os
import traceback
from datetime import datetime
from typing import Optional

import pytz
from fastapi import APIRouter, Query

from app.database import get_pg_buylist_database

POSTGRES_URL_BUYLIST = os.getenv("POSTGRES_URL_BUYLIST")
ALLOWED_STATUSES = ['Confirm Sales', 'Get Paid', 'Upload Transfer Receipts']

router = APIRouter(prefix="/subs")


@router.get("/report")
async def get_unclaimed_sales(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        start_date: Optional[str] = Query(None),
        start_hr: Optional[int] = Query(1),
        end_date: Optional[str] = Query(None),
        end_hr: Optional[int] = Query(24),
        weekday: Optional[int] = Query(7),
        graph: Optional[str] = Query('each'),
        type: Optional[str] = Query('unclaimed'),
        track_interval: Optional[int] = Query(None)
):
    try:

        start_hr = f'0{start_hr - 1}' if start_hr <= 10 else f'{start_hr - 1}'
        end_hr = f'0{end_hr - 1}' if end_hr <= 10 else f'{end_hr - 1}'
        start_datetime = await _get_converted_time(f"{start_date} {start_hr}:00:00", timezone)
        end_datetime = await _get_converted_time(f"{end_date} {end_hr}:59:59", timezone)
        if type == 'buylist':
            return await _get_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval,
                                                       weekday)
        else:
            return await _get_subs_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval,
                                                            weekday)
    except Exception as e:
        traceback.print_exc()
        raise e


@router.get("/top")
async def _get_data_for_graph_avg_type(
        page: Optional[int] = Query(1),
        page_size: Optional[int] = Query(20),
        start_date: Optional[str] = Query(None),
        end_date: Optional[str] = Query(None),
        start_hr: Optional[int] = Query(1),
        end_hr: Optional[int] = Query(23),
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        )
):
    start_datetime = await _get_converted_time(f"{start_date} {start_hr}:00:00", timezone)
    end_datetime = await _get_converted_time(f"{end_date} {end_hr}:59:59", timezone)

    pg_query = f"""
                    select sales_source as platform
                        ,max(event_name) as event_name
                        ,count(1) as total_subs
                        ,max(venue) as venue
                        ,max(event_date) as event_date
                        ,max(venue_city) as city
                        ,max(coalesce(event_state,'')) as state
                        ,max(coalesce(link,'')) as event_url
                    from shadows_buylist 
                        where subs=1 and event_date>current_date
                        and date_trunc('hour', created_at AT TIME ZONE 'CDT' AT TIME ZONE 'UTC')>='{start_datetime}'
                        and date_trunc('hour', created_at AT TIME ZONE 'CDT' AT TIME ZONE 'UTC')<='{end_datetime}'
                    group by event_code,sales_source
                    order by total_subs desc
                    limit {page_size}
                    offset {page_size * (page - 1)}
                    ;
                """
    count_query = f"""
                    select count(1) as cnt
                    from(select 1 from shadows_buylist 
                        where subs=1 and event_date>current_date
                        and date_trunc('hour', created_at AT TIME ZONE 'CDT' AT TIME ZONE 'UTC')>='{start_datetime}'
                        and date_trunc('hour', created_at AT TIME ZONE 'CDT' AT TIME ZONE 'UTC')<='{end_datetime}'
                        group by event_code,sales_source)
    """
    print(pg_query)
    print(count_query)
    pg_results = await get_pg_buylist_database().fetch_all(pg_query)
    total_count = await get_pg_buylist_database().fetch_one(count_query)
    count = total_count[0] if total_count else 0
    return {
        "items": [dict(r) for r in pg_results],
        "total": count
    }


async def _get_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval, weekday):
    filter = f"and EXTRACT(DOW FROM (ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}')) = {weekday}"
    pg_query = f"""
            with sales as (
            select id,amount as amount,created_at as created_at from viagogo_sales
            union all
            select id,total_payout as amount,create_time as created_at from gotickets_sales
            union all
            select id,total as amount,created as created_at from seatgeek_sales

        )
        SELECT
            TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'YYYY-MM-DD') AS date,
            TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'HH24:MI') AS time,
            count(1) as value,
            coalesce(sum(amount), 0) as sales_cost
        FROM generate_series(
            TIMESTAMP '{start_datetime}',
            TIMESTAMP '{end_datetime}',
            '{track_interval} minutes'::interval
        ) AS ts
        left join sales s on date_trunc('hour', s.created_at)=ts
        WHERE 1=1
        {filter if weekday < 7 else ""} 
        group by ts
        ORDER BY date, time;
        """
    pg_results = await get_pg_buylist_database().fetch_all(pg_query)
    return [dict(r) for r in pg_results]


async def _get_subs_data_for_graph_each_type(end_datetime, start_datetime, timezone, track_interval, weekday):
    filter = f"and EXTRACT(DOW FROM (ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}')) = {weekday}"
    pg_query = f"""
         SELECT
            TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'YYYY-MM-DD') AS date,
            TO_CHAR((ts AT TIME ZONE 'UTC' AT TIME ZONE '{timezone}'), 'HH24:MI') AS time,
            sum(coalesce(s.subs,0)) as value,
            SUM(CASE WHEN s.subs = 1 THEN COALESCE(s.amount, 0) ELSE 0 END) as sales_cost
        FROM generate_series(
            TIMESTAMP '{start_datetime}',
            TIMESTAMP '{end_datetime}',
            '{track_interval} minutes'::interval
        ) AS ts
        left join shadows_buylist s on date_trunc('hour',s.created_at AT TIME ZONE 'CDT' AT TIME ZONE 'UTC')=ts
        WHERE 1=1
            {filter if weekday < 7 else ""} 
        group by ts
        ORDER BY date, time;
        """
    pg_results = await get_pg_buylist_database().fetch_all(pg_query)
    return [dict(r) for r in pg_results]


async def _get_converted_time(dt_str, tmz):
    origin_tz = pytz.timezone(tmz)
    local_dt = origin_tz.localize(datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S"))
    utc_dt = local_dt.astimezone(pytz.UTC)
    utc_str = utc_dt.strftime("%Y-%m-%d %H:%M:%S")
    return utc_str
