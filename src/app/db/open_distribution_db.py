import json
import os
from datetime import datetime
from typing import Any

import requests
import snowflake.connector
from fastapi import HTTPException

from app.database import (
    get_pg_open_distribution_readonly_database,
    get_pg_open_distribution_database,
    get_snowflake_connection,
)
from app.model.open_distribution_models import (
    ShowDetailsModel,
    UnmappedEventModel,
    ShowModel,
    TradeDeskEventModel,
    TradeDeskEventVenueModel,
    TradeDeskEventCityModel,
    StubhubEventModel,
    StubhubEventVenueModel,
    StubhubEventCityModel,
    MapEventRequest,
    MapStubhubEventRequest,
)
from app.time_utils.timezone_utils import get_timezone_from_location
from app.service.sync_trigger_service import SyncTriggerService


async def get_all_shows():
    try:
        query = """
            SELECT * FROM open_dist.outbox_show
            ORDER BY title ASC;
            """
        result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shows_details() -> list[ShowDetailsModel]:
    try:
        query = """
            WITH show_events AS (
                SELECT 
                    os.id as show_id,
                    os.title as show_name,
                    os.description,
                    MIN(oe.event_start) as show_start,
                    MAX(oe.event_start) as show_end
                FROM open_dist.outbox_show os
                LEFT JOIN open_dist.outbox_event oe ON os.id = oe.show_id
                    AND oe.updated_at > current_timestamp - interval '24 hours'
                GROUP BY os.id, os.title, os.description
            ),
            show_markup AS (
                SELECT 
                    outbox_show_id,
                    ROUND(AVG(markup_percent), 2) as average_markup
                FROM (
                    SELECT outbox_show_id, markup_percent
                    FROM open_dist.outbox_sync_config
                    WHERE markup_percent IS NOT NULL
                    UNION ALL
                    SELECT outbox_show_id, markup_percent
                    FROM open_dist.outbox_stubhub_sync_config
                    WHERE markup_percent IS NOT NULL
                ) combined_markup
                GROUP BY outbox_show_id
            ),
            show_listings AS (
                SELECT 
                    oe.show_id,
                    COUNT(ocl.id) as total_listings
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_current_listing ocl ON ocl.event_id = oe.id 
                    AND (
                        ocl.collection_time > CURRENT_TIMESTAMP - interval '35 minutes'
                        OR (ocl.collection_time IS NULL AND ocl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes')
                    )
                GROUP BY oe.show_id
            ),
            active_listings AS (
                SELECT 
                    oe.show_id,
                    COUNT(DISTINCT tcl.id) + COUNT(DISTINCT scl.id) as active_listings_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.trade_desk_current_listing tcl ON tcl.event_id = osc.trade_desk_event_id::TEXT
                    AND tcl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes'
                    AND tcl.ticket_status = 'Active'
                    AND tcl.seats_available != '[]'
                LEFT JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
                LEFT JOIN open_dist.stubhub_current_listing scl ON scl.stubhub_event_id = sosc.stubhub_event_id
                    AND scl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes'
                    AND scl.ticket_status = 'Active'
                GROUP BY oe.show_id
            ),
            show_synced_events AS (
                SELECT 
                    oe.show_id as outbox_show_id,
                    COUNT(CASE WHEN osc.sync_active = true THEN 1 END) + COUNT(CASE WHEN sosc.sync_active = true THEN 1 END) as synced_events_count
                FROM open_dist.outbox_event oe 
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
                WHERE oe.updated_at > current_timestamp - interval '24 hours'
                GROUP BY oe.show_id
            ),
            show_sales AS (
                SELECT 
                    osc.outbox_show_id,
                    COUNT(so.id) as sales_count,
                    COALESCE(SUM(so.price), 0) as sales_total_price
                FROM open_dist.outbox_sync_config osc
                LEFT JOIN open_dist.trade_desk_sales_order so ON so.event_id = osc.trade_desk_event_id
                    AND so.ship_status = 1
                GROUP BY osc.outbox_show_id
            ),
            show_mapping AS (
                SELECT 
                    oe.show_id,
                    COUNT(oe.id) as total_events,
                    COUNT(osc.trade_desk_event_id) + COUNT(sosc.stubhub_event_id) as mapped_events
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
                WHERE oe.updated_at > current_timestamp - interval '24 hours'
                AND oe.event_start > current_timestamp + interval '6 hours'
                GROUP BY oe.show_id
            )
            SELECT 
                se.show_id as id,
                se.show_name,
                se.description,
                COALESCE(smap.mapped_events, 0) as mapped_events_count,
                se.show_start,
                se.show_end,
                COALESCE(sm.average_markup, 0) as average_markup,
                COALESCE(sl.total_listings, 0) as total_listings,
                COALESCE(sse.synced_events_count, 0) as synced_events_count,
                COALESCE(ss.sales_count, 0) as sales_count,
                COALESCE(al.active_listings_count, 0) as active_listings_count,
                COALESCE(ss.sales_total_price, 0) as sales_total_price,
                CASE 
                    WHEN COALESCE(sse.synced_events_count, 0) > 0 THEN 'Active'
                    ELSE 'Inactive'
                END as status,
                CONCAT(COALESCE(smap.mapped_events, 0), ' out of ', COALESCE(smap.total_events, 0), ' Mapped') as map_info
            FROM show_events se
            LEFT JOIN show_markup sm ON se.show_id = sm.outbox_show_id
            LEFT JOIN show_listings sl ON se.show_id = sl.show_id
            LEFT JOIN show_synced_events sse ON se.show_id = sse.outbox_show_id
            LEFT JOIN show_sales ss ON se.show_id = ss.outbox_show_id
            LEFT JOIN show_mapping smap ON se.show_id = smap.show_id
            LEFT JOIN active_listings al ON se.show_id = al.show_id
            ORDER BY se.show_name ASC;
            """
        result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        return [ShowDetailsModel(**row._mapping) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shows_events(show_id: str):
    try:
        query = """
            WITH event_listings AS (
                SELECT 
                    oe.id as event_id,
                    COUNT(ocl.id) as total_listings_count,
                    COALESCE(SUM(
                        CASE 
                            WHEN ocl.seats IS NOT NULL AND ocl.seats != '[]' 
                            THEN array_length(string_to_array(trim(both '[]' from ocl.seats), ','), 1)
                            ELSE 0 
                        END
                    ), 0) as total_seats_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_current_listing ocl ON ocl.event_id = oe.id
                    AND (
                        ocl.collection_time > CURRENT_TIMESTAMP - interval '35 minutes'
                        OR (ocl.collection_time IS NULL AND ocl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes')
                    )
                WHERE oe.show_id = :show_id
                GROUP BY oe.id
            ),
            event_active_listings_td AS (
                SELECT 
                    oe.id as event_id,
                    COUNT(tcl.id) as active_listings_count,
                    COALESCE(SUM(
                        CASE 
                            WHEN tcl.seats_available IS NOT NULL AND tcl.seats_available != '[]' 
                            THEN array_length(string_to_array(trim(both '[]' from tcl.seats_available), ','), 1)
                            ELSE 0 
                        END
                    ), 0) as active_seats_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.trade_desk_current_listing tcl ON tcl.event_id = osc.trade_desk_event_id::TEXT
                    AND tcl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes'
                    AND tcl.ticket_status = 'Active'
                    AND tcl.seats_available != '[]'
                WHERE oe.show_id = :show_id
                GROUP BY oe.id
            ),
            event_active_listings_sh AS (
                SELECT 
                    oe.id as event_id,
                    COUNT(scl.id) as active_listings_count,
                    COALESCE(SUM(scl.quantity), 0) as active_seats_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
                LEFT JOIN open_dist.stubhub_current_listing scl ON scl.stubhub_event_id = sosc.stubhub_event_id
                    AND scl.updated_at > CURRENT_TIMESTAMP - interval '4 minutes'
                    AND scl.ticket_status = 'Active'
                WHERE oe.show_id = :show_id
                GROUP BY oe.id
            ),
            event_active_listings AS (
                SELECT 
                    COALESCE(td.event_id, sh.event_id) as event_id,
                    COALESCE(td.active_listings_count, 0) + COALESCE(sh.active_listings_count, 0) as active_listings_count,
                    COALESCE(td.active_seats_count, 0) + COALESCE(sh.active_seats_count, 0) as active_seats_count
                FROM event_active_listings_td td
                FULL OUTER JOIN event_active_listings_sh sh ON td.event_id = sh.event_id
            ),
            event_sales AS (
                SELECT 
                    oe.id as event_id,
                    COUNT(so.id) as shipped_event_sales_orders_count,
                    COALESCE(SUM(so.price), 0) as event_sales_total_price,
                    COALESCE(SUM(
                        CASE 
                            WHEN so.items IS NOT NULL 
                            THEN (
                                SELECT SUM(
                                    CASE 
                                        WHEN item->>'seats' IS NOT NULL 
                                        THEN jsonb_array_length(item->'seats')
                                        ELSE 0 
                                    END
                                )
                                FROM jsonb_array_elements(so.items) as item
                            )
                            ELSE 0 
                        END
                    ), 0) as shipped_event_sales_orders_seats_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.trade_desk_sales_order so ON so.event_id = osc.trade_desk_event_id
                    AND so.ship_status = 1
                WHERE oe.show_id = :show_id
                GROUP BY oe.id
            ),
            event_delayed_orders AS (
                SELECT 
                    oe.id as event_id,
                    COUNT(so.id) as delayed_orders_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                LEFT JOIN open_dist.trade_desk_sales_order so ON so.event_id = osc.trade_desk_event_id
                    AND so.ship_status != 1
                WHERE oe.show_id = :show_id
                GROUP BY oe.id
            ),
            trade_desk_events AS (
                SELECT 
                    oe.id,
                    oe.title,
                    oe.event_start,
                    COALESCE(el.total_listings_count, 0) as total_listings_count,
                    COALESCE(el.total_seats_count, 0) as total_seats_count,
                    COALESCE(osc.markup_percent, 0) as event_markup,
                    (
                        SELECT lro.action_value
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_value,
                    (
                        SELECT lro.timing_type
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_type,
                    (
                        SELECT lro.timing_from_hours
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_from_hours,
                    (
                        SELECT lro.timing_to_hours
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_to_hours,
                    COALESCE(eal.active_listings_count, 0) as active_listings_count,
                    COALESCE(eal.active_seats_count, 0) as active_seats_count,
                    COALESCE(es.shipped_event_sales_orders_count, 0) as shipped_event_sales_orders_count,
                    COALESCE(es.shipped_event_sales_orders_seats_count, 0) as shipped_event_sales_orders_seats_count,
                    COALESCE(es.event_sales_total_price, 0) as event_sales_total_price,
                    COALESCE(edo.delayed_orders_count, 0) as delayed_orders_count,
                    CASE 
                        WHEN osc.sync_active = true THEN 'Active'
                        WHEN osc.sync_active = false THEN 'Inactive'
                        ELSE 'Unknown'
                    END as event_status,
                    'TradeDesk' as source,
                    osc.trade_desk_event_id as source_event_id,
                    osc.trade_desk_event_name as event_name
                FROM open_dist.outbox_event oe
                JOIN open_dist.outbox_show os ON os.id = oe.show_id
                INNER JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                    AND osc.trade_desk_event_id IS NOT NULL
                LEFT JOIN event_listings el ON el.event_id = oe.id
                LEFT JOIN event_active_listings eal ON eal.event_id = oe.id
                LEFT JOIN event_sales es ON es.event_id = oe.id
                LEFT JOIN event_delayed_orders edo ON edo.event_id = oe.id
                WHERE oe.show_id = :show_id
                AND oe.updated_at > current_timestamp - interval '24 hours'
            ),
            stubhub_events AS (
                SELECT 
                    oe.id,
                    oe.title,
                    oe.event_start,
                    COALESCE(el.total_listings_count, 0) as total_listings_count,
                    COALESCE(el.total_seats_count, 0) as total_seats_count,
                    COALESCE(sosc.markup_percent, 0) as event_markup,
                    (
                        SELECT lro.action_value
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_value,
                    (
                        SELECT lro.timing_type
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_type,
                    (
                        SELECT lro.timing_from_hours
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_from_hours,
                    (
                        SELECT lro.timing_to_hours
                        FROM open_dist.listing_rule_override lro
                        WHERE lro.is_active = true
                          AND lro.action_type = 'mark_override'
                          AND (
                              (lro.show_filter_type = 'all') OR
                              (lro.show_filter_type = 'specific_shows' 
                               AND lro.show_ids::text[] @> ARRAY[oe.show_id::TEXT]) OR
                              (lro.show_filter_type = 'specific_events' 
                               AND lro.event_ids @> ARRAY[oe.id::TEXT])
                          )
                          AND (
                              lro.timing_type = 'entire_event'
                              OR
                              (lro.timing_type = 'hours_before_event'
                               AND oe.event_start IS NOT NULL
                               AND CURRENT_TIMESTAMP >= oe.event_start - INTERVAL '1 hour' * lro.timing_to_hours
                               AND CURRENT_TIMESTAMP < oe.event_start - INTERVAL '1 hour' * lro.timing_from_hours)
                          )
                        ORDER BY lro.priority_order ASC
                        LIMIT 1
                    ) as markup_override_timing_to_hours,
                    COALESCE(eal.active_listings_count, 0) as active_listings_count,
                    COALESCE(eal.active_seats_count, 0) as active_seats_count,
                    COALESCE(es.shipped_event_sales_orders_count, 0) as shipped_event_sales_orders_count,
                    COALESCE(es.shipped_event_sales_orders_seats_count, 0) as shipped_event_sales_orders_seats_count,
                    COALESCE(es.event_sales_total_price, 0) as event_sales_total_price,
                    COALESCE(edo.delayed_orders_count, 0) as delayed_orders_count,
                    CASE 
                        WHEN sosc.sync_active = true THEN 'Active'
                        WHEN sosc.sync_active = false THEN 'Inactive'
                        ELSE 'Unknown'
                    END as event_status,
                    'Stubhub' as source,
                    sosc.stubhub_event_id as source_event_id,
                    sosc.stubhub_event_name as event_name
                FROM open_dist.outbox_event oe
                JOIN open_dist.outbox_show os ON os.id = oe.show_id
                INNER JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
                    AND sosc.stubhub_event_id IS NOT NULL
                LEFT JOIN event_listings el ON el.event_id = oe.id
                LEFT JOIN event_active_listings eal ON eal.event_id = oe.id
                LEFT JOIN event_sales es ON es.event_id = oe.id
                LEFT JOIN event_delayed_orders edo ON edo.event_id = oe.id
                WHERE oe.show_id = :show_id
                AND oe.updated_at > current_timestamp - interval '24 hours'
            )
            SELECT * FROM trade_desk_events
            UNION ALL
            SELECT * FROM stubhub_events
            ORDER BY event_start ASC
            """
        result = await get_pg_open_distribution_readonly_database().fetch_all(
            query=query, values={"show_id": show_id}
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_sync_active_by_show_id(show_id: str, sync_active: bool):
    """Update sync_active for all events in a show (TradeDesk only)."""
    try:
        query = """
            UPDATE open_dist.outbox_sync_config 
            SET sync_active = :sync_active
            WHERE outbox_show_id = :show_id
            RETURNING outbox_event_id, outbox_show_id;
        """
        result = await get_pg_open_distribution_database().fetch_all(
            query=query, values={"show_id": show_id, "sync_active": sync_active}
        )
        result_dict = [dict(row) for row in result]
        
        # ✅ NEW: Trigger immediate sync for each event in the show
        try:
            sync_trigger = SyncTriggerService()
            
            for row in result_dict:
                event_id = row["outbox_event_id"]
                await sync_trigger.trigger_immediate_sync(
                    event_id=event_id,
                    sync_active=sync_active,
                )
        except Exception as trigger_error:
            # Log but don't fail the request if SQS trigger fails
            print(f"⚠️ Warning: Failed to trigger sync for show {show_id}: {trigger_error}")

        
        return result_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_stubhub_sync_active_by_show_id(show_id: str, sync_active: bool):
    """Update sync_active for all events in a show (StubHub only)."""
    try:
        query = """
            UPDATE open_dist.outbox_stubhub_sync_config 
            SET sync_active = :sync_active
            WHERE outbox_show_id = :show_id
            RETURNING outbox_event_id, outbox_show_id;
        """
        result = await get_pg_open_distribution_database().fetch_all(
            query=query, values={"show_id": show_id, "sync_active": sync_active}
        )
        result_dict = [dict(row) for row in result]
        return result_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_sync_active_by_event_id(outbox_event_id: str, sync_active: bool):
    """Update sync_active for a specific event."""
    try:
        query = """
            UPDATE open_dist.outbox_sync_config 
            SET sync_active = :sync_active
            WHERE outbox_event_id = :outbox_event_id
            RETURNING outbox_event_id;
        """
        result = await get_pg_open_distribution_database().fetch_one(
            query=query,
            values={"outbox_event_id": outbox_event_id, "sync_active": sync_active},
        )

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Event not found or no sync config exists for this event.",
            )

        result_dict = dict(result)
        
        # ✅ NEW: Trigger immediate sync via SQS (two messages: immediate + 5min delayed)
        try:
            sync_trigger = SyncTriggerService()
            await sync_trigger.trigger_immediate_sync(
                event_id=outbox_event_id,
                sync_active=sync_active,
            )
        except Exception as trigger_error:
            # Log but don't fail the request if SQS trigger fails
            print(f"⚠️ Warning: Failed to trigger sync for event {outbox_event_id}: {trigger_error}")

        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def bulk_update_by_event_ids(
        outbox_event_ids: list[str], sync_active: bool | None, markup_percent: float | None
):
    """Bulk update sync_active and/or markup_percent for multiple events."""
    try:
        if not outbox_event_ids:
            raise HTTPException(status_code=400, detail="No event IDs provided.")

        # Validate that at least one field is provided
        if sync_active is None and markup_percent is None:
            raise HTTPException(
                status_code=400,
                detail="At least one field (sync_active or markup_percent) must be provided.",
            )

        # Create placeholders for the IN clause
        placeholders = ", ".join([f":id{i}" for i in range(len(outbox_event_ids))])

        # Build the SET clause dynamically based on which fields are provided
        set_clauses = []
        values: dict[str, bool | float | str] = {}

        if sync_active is not None:
            set_clauses.append("sync_active = :sync_active")
            values["sync_active"] = sync_active

        if markup_percent is not None:
            set_clauses.append("markup_percent = :markup_percent")
            values["markup_percent"] = markup_percent

        # Add individual ID parameters
        for i, event_id in enumerate(outbox_event_ids):
            values[f"id{i}"] = event_id

        query = f"""
            UPDATE open_dist.outbox_sync_config 
            SET {", ".join(set_clauses)}
            WHERE outbox_event_id IN ({placeholders})
            RETURNING outbox_event_id;
        """

        result = await get_pg_open_distribution_database().fetch_all(query=query, values=values)

        result_dict = [dict(row) for row in result]
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def bulk_update_stubhub_by_event_ids(
        outbox_event_ids: list[str], sync_active: bool | None, markup_percent: float | None
):
    """Bulk update sync_active and/or markup_percent for multiple StubHub events."""
    try:
        if not outbox_event_ids:
            raise HTTPException(status_code=400, detail="No event IDs provided.")

        # Validate that at least one field is provided
        if sync_active is None and markup_percent is None:
            raise HTTPException(
                status_code=400,
                detail="At least one field (sync_active or markup_percent) must be provided.",
            )

        # Create placeholders for the IN clause
        placeholders = ", ".join([f":id{i}" for i in range(len(outbox_event_ids))])

        # Build the SET clause dynamically based on which fields are provided
        set_clauses = []
        values: dict[str, bool | float | str] = {}

        if sync_active is not None:
            set_clauses.append("sync_active = :sync_active")
            values["sync_active"] = sync_active

        if markup_percent is not None:
            set_clauses.append("markup_percent = :markup_percent")
            values["markup_percent"] = markup_percent

        # Add individual ID parameters
        for i, event_id in enumerate(outbox_event_ids):
            values[f"id{i}"] = event_id

        query = f"""
            UPDATE open_dist.outbox_stubhub_sync_config 
            SET {", ".join(set_clauses)}
            WHERE outbox_event_id IN ({placeholders})
            RETURNING outbox_event_id;
        """

        result = await get_pg_open_distribution_database().fetch_all(query=query, values=values)

        result_dict = [dict(row) for row in result]
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_unmapped_events() -> list[UnmappedEventModel]:
    try:
        query = """
            SELECT 
                oe.id,
                oe.title,
                oe.event_start,
                jsonb_build_object(
                        'id', os.id,
                        'title', os.title,
                        'description', os.description,
                        'created_at', os.created_at,
                        'updated_at', os.updated_at
                    ) AS show,
                oe.created_at,
                oe.updated_at,
                (osc.outbox_event_id IS NULL) AS is_unmapped_in_trade_desk,
                (sosc.outbox_event_id IS NULL) AS is_unmapped_in_stubhub,
                CASE 
                    WHEN osc.outbox_event_id IS NOT NULL THEN 'TradeDesk'
                    WHEN sosc.outbox_event_id IS NOT NULL THEN 'StubHub'
                    ELSE 'Unmapped'
                END AS mapped_to
            FROM open_dist.outbox_event oe
            JOIN open_dist.outbox_show os ON os.id = oe.show_id
            LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
            LEFT JOIN open_dist.outbox_stubhub_sync_config sosc ON sosc.outbox_event_id = oe.id
            WHERE (oe.updated_at IS NULL OR
			oe.updated_at > current_timestamp - interval '24 hours')
            AND oe.event_start > current_timestamp + interval '6 hours'
            AND (osc.outbox_event_id IS NULL OR sosc.outbox_event_id IS NULL)
            ORDER BY oe.event_start ASC
        """
        result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        unmapped_events = []

        for row in result:
            row_dict = dict(row)
            show_data = json.loads(row_dict["show"])
            unmapped_events.append(
                UnmappedEventModel(
                    id=row_dict["id"],
                    title=row_dict["title"],
                    event_start=row_dict["event_start"],
                    show=ShowModel(**show_data),
                    created_at=row_dict["created_at"],
                    updated_at=row_dict["updated_at"],
                    is_unmapped_in_trade_desk=row_dict["is_unmapped_in_trade_desk"],
                    is_unmapped_in_stubhub=row_dict["is_unmapped_in_stubhub"],
                    mapped_to=row_dict["mapped_to"],
                )
            )

        return unmapped_events
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_trade_desk_events(
        event_date: str, show_name: str
) -> list[TradeDeskEventModel]:
    try:
        # Get broker key from environment variable
        broker_key = os.getenv("TRADE_DESK_BROKER_KEY")

        # Build query parameters
        params = {
            "name": show_name,
            "nameMatch": "contains",
            "dateFrom": event_date,
            "dateTo": event_date,
            "includeResultCount": "1",
        }

        # Make the API request
        response = requests.get(
            "https://api.zerohero.com/v1/events",
            params=params,
            headers={
                "Broker-Key": broker_key,
                "Content-Type": "application/json",
            },
            timeout=30,  # 30 second timeout
        )

        # Check if request was successful
        response.raise_for_status()

        # Parse the response
        data = response.json()

        # Extract events from response
        events_data = data.get("resultData", [])

        # Convert to TradeDeskEventModel objects
        result = []
        for event_data in events_data:
            try:
                # Extract venue data
                venue_data = event_data.get("venue", {})
                city_data = venue_data.get("city", {})
                
                city_name = city_data.get("name", "")
                state_name = city_data.get("state", "")
                country_name = venue_data.get("country", "")
                
                # Get timezone from location (using geopy + timezonefinder)
                # Falls back to UTC if geocoding fails
                event_timezone = get_timezone_from_location(
                    city=city_name,
                    state=state_name,
                    country=country_name
                )
                
                # Fallback to UTC if timezone detection fails
                if not event_timezone:
                    event_timezone = "UTC"
                    print(f"WARNING: Could not determine timezone for event {event_data.get('id')}, "
                          f"venue: {city_name}, {state_name}, {country_name}. Defaulting to UTC.")
                
                trade_desk_event = TradeDeskEventModel(
                    id=event_data.get("id"),
                    name=event_data.get("name"),
                    date=event_data.get("date"),
                    venue=TradeDeskEventVenueModel(
                        id=venue_data.get("id"),
                        name=venue_data.get("name"),
                        also_called=venue_data.get("alsoCalled", ""),
                        country=country_name,
                        city=TradeDeskEventCityModel(
                            id=city_data.get("id"),
                            name=city_name,
                            state=state_name,
                            state_code=city_data.get("stateCode", ""),
                        ),
                    ),
                    timezone=event_timezone,
                )
                result.append(trade_desk_event)
            except Exception as e:
                # Log the error but continue processing other events
                print(f"Error parsing event data: {e}, Event: {event_data}")
                continue

        return result

    except requests.exceptions.RequestException as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch TradeDesk events: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_stubhub_events(
        event_date: str, show_name: str
) -> list[StubhubEventModel]:
    try:
        # Parse event_date to extract date part for Snowflake query
        # Handle different date formats (ISO, MM/DD/YYYY, etc.)
        try:
            # Try parsing as ISO format first
            if "T" in event_date or "Z" in event_date:
                parsed_date = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
                date_str = parsed_date.strftime("%Y-%m-%d")
            else:
                # Try parsing as other common formats
                try:
                    parsed_date = datetime.strptime(event_date.split()[0], "%m/%d/%Y")
                    date_str = parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    # If parsing fails, use the date as-is (assuming it's already in YYYY-MM-DD format)
                    date_str = event_date.split()[0] if " " in event_date else event_date
        except Exception:
            # Fallback: use the original event_date
            date_str = event_date.split()[0] if " " in event_date else event_date
            print(f"WARNING: Could not parse event_date '{event_date}', using as-is: {date_str}")

        # Build parameterized Snowflake query
        query = """
            SELECT 
                se.stubhubeventid,
                se.stubhubeventname,
                se.eventdate,
                ve.venuename,
                ve.city,
                ve.state,
                ve.country,
                ve.postalcode
            FROM ticketboat.public.stubhub_events se
            JOIN ticketboat.public.stubhub_venues ve ON ve.venueid = se.venueid
            WHERE 
                se.stubhubeventname ILIKE %s 
                AND se.eventdate::DATE = %s::DATE;
        """
        
        # Parameterize the query
        show_name_pattern = f"%{show_name}%"
        params = (show_name_pattern, date_str)

        # Execute query using Snowflake connection
        with get_snowflake_connection().cursor(snowflake.connector.DictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        # Convert to StubhubEventModel objects
        result = []
        for row in rows:
            try:
                # Convert row keys to lowercase for easier access (Snowflake returns uppercase)
                row_lower = {k.lower(): v for k, v in row.items()}
                
                # Extract data from Snowflake row
                event_id = row_lower.get("stubhubeventid")
                if event_id is None:
                    event_id = 0
                event_name = row_lower.get("stubhubeventname") or ""
                event_date_value = row_lower.get("eventdate")
                venue_name = row_lower.get("venuename") or ""
                city_name = row_lower.get("city") or ""
                state_name = row_lower.get("state") or ""
                country_name = row_lower.get("country") or ""
                
                # Format event date as string: MM/DD/YYYY HH:MM:SS (24-hour format)
                if event_date_value:
                    if isinstance(event_date_value, datetime):
                        # Format as MM/DD/YYYY 15:30:00
                        date_str_formatted = event_date_value.strftime("%m/%d/%Y %H:%M:%S")
                    else:
                        date_str_formatted = str(event_date_value)
                else:
                    date_str_formatted = date_str
                
                # Hardcode timezone to CST (America/Chicago)
                event_timezone = "America/Chicago"
                
                # Derive state_code from state (first 2 characters, uppercase)
                state_code = state_name[:2].upper() if state_name else ""
                
                stubhub_event = StubhubEventModel(
                    id=event_id,
                    name=event_name,
                    date=date_str_formatted,
                    venue=StubhubEventVenueModel(
                        id=0,  # Snowflake query doesn't return venue ID
                        name=venue_name,
                        also_called="",  # Snowflake query doesn't return also_called
                        country=country_name,
                        city=StubhubEventCityModel(
                            id=0,  # Snowflake query doesn't return city ID
                            name=city_name,
                            state=state_name,
                            state_code=state_code,
                        ),
                    ),
                    timezone=event_timezone,
                )
                result.append(stubhub_event)
            except Exception as e:
                # Log the error but continue processing other events
                print(f"Error parsing event data: {e}, Row: {row}")
                continue

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def map_events(data: MapEventRequest):
    try:
        # Parse the event_date string to datetime object
        event_datetime = datetime.fromisoformat(data.event_date.replace("Z", "+00:00"))

        query = """
            INSERT INTO open_dist.outbox_sync_config (
                id, trade_desk_event_id, outbox_event_id, sync_active, markup_percent, outbox_event_start, outbox_show_id, trade_desk_event_name, event_timezone
            ) VALUES (
                uuid_generate_v4(), :trade_desk_event_id, :outbox_event_id, true, 10.00, :event_date, :outbox_show_id, :trade_desk_event_name, :event_timezone
            ) RETURNING *;
        """
        values = {
            "trade_desk_event_id": data.trade_desk_event_id,
            "outbox_event_id": data.outbox_event_id,
            "event_date": event_datetime,  # Now passing datetime object instead of string
            "outbox_show_id": data.outbox_show_id,
            "trade_desk_event_name": data.trade_desk_event_name,
            "event_timezone": data.timezone,  # Store IANA timezone identifier
        }

        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Failed to create mapping")

        result_dict = dict(result)
        
        # ✅ NEW: Trigger immediate inventory fetch for newly mapped event
        try:
            sync_trigger = SyncTriggerService()
            await sync_trigger.trigger_inventory_fetch(event_id=data.outbox_event_id)
            print(f"✅ Triggered inventory fetch for newly mapped event {data.outbox_event_id}")
        except Exception as trigger_error:
            # Log but don't fail the request if SQS trigger fails
            print(f"⚠️ Warning: Failed to trigger inventory fetch for event {data.outbox_event_id}: {trigger_error}")
        
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def map_stubhub_events(data: MapStubhubEventRequest):
    try:
        # Parse the event_date string to datetime object
        event_datetime = datetime.fromisoformat(data.event_date.replace("Z", "+00:00"))

        query = """
            INSERT INTO open_dist.outbox_stubhub_sync_config (
                id, stubhub_event_id, stubhub_event_name, outbox_event_id, sync_active, markup_percent, outbox_event_start, event_timezone, outbox_show_id
            ) VALUES (
                uuid_generate_v4(), :stubhub_event_id, :stubhub_event_name, :outbox_event_id, false, 10.00, :event_date, :event_timezone, :outbox_show_id
            ) RETURNING *;
        """
        values = {
            "stubhub_event_id": data.stubhub_event_id,
            "stubhub_event_name": data.stubhub_event_name,
            "outbox_event_id": data.outbox_event_id,
            "event_date": event_datetime,  # Now passing datetime object instead of string
            "outbox_show_id": data.outbox_show_id,
            "event_timezone": data.timezone,  # Store IANA timezone identifier
        }

        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Failed to create StubHub mapping")

        result_dict = dict(result)
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def unmap_event(outbox_event_id: str):
    """Delete the sync configuration for an event, effectively unmapping it from TradeDesk."""
    try:
        query = """
            DELETE FROM open_dist.outbox_sync_config
            WHERE outbox_event_id = :outbox_event_id
            RETURNING *
        """
        values = {"outbox_event_id": outbox_event_id}
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Event mapping not found")
        
        result_dict = dict(result)
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def unmap_stubhub_event(outbox_event_id: str):
    """Delete the StubHub sync configuration for an event, effectively unmapping it from StubHub."""
    try:
        query = """
            DELETE FROM open_dist.outbox_stubhub_sync_config
            WHERE outbox_event_id = :outbox_event_id
            RETURNING *
        """
        values = {"outbox_event_id": outbox_event_id}
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="StubHub event mapping not found")
        
        result_dict = dict(result)
        return result_dict
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# =====================================================
# Rule Override Functions
# =====================================================

async def get_all_rule_overrides(is_active=None):
    """Get all rule overrides, optionally filtered by active status."""
    try:
        if is_active is not None:
            query = """
                SELECT * FROM open_dist.listing_rule_override
                WHERE is_active = :is_active
                ORDER BY priority_order ASC
            """
            values = {"is_active": is_active}
            result = await get_pg_open_distribution_readonly_database().fetch_all(query=query, values=values)
        else:
            query = """
                SELECT * FROM open_dist.listing_rule_override
                ORDER BY priority_order ASC
            """
            result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_rule_override_by_id(rule_id: str):
    """Get a single rule override by ID."""
    try:
        query = """
            SELECT * FROM open_dist.listing_rule_override
            WHERE id = :rule_id
        """
        values = {"rule_id": rule_id}
        result = await get_pg_open_distribution_readonly_database().fetch_one(query=query, values=values)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def create_rule_override(rule_data: dict):
    """Create a new rule override."""
    try:
        # Get the next priority order
        max_priority_query = """
            SELECT COALESCE(MAX(priority_order), 0) + 1 as next_priority
            FROM open_dist.listing_rule_override
        """
        max_result = await get_pg_open_distribution_database().fetch_one(query=max_priority_query)
        next_priority = max_result["next_priority"]
        
        query = """
            INSERT INTO open_dist.listing_rule_override (
                id, priority_order, show_filter_type, show_ids, event_ids,
                seat_filter_type, action_type, action_value, timing_type, timing_from_hours, timing_to_hours,
                is_active, notes, created_at, updated_at
            ) VALUES (
                uuid_generate_v4(), :priority_order, :show_filter_type, :show_ids, :event_ids,
                :seat_filter_type, :action_type, :action_value, :timing_type, :timing_from_hours, :timing_to_hours,
                :is_active, :notes, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING *
        """
        values = {
            "priority_order": next_priority,
            "show_filter_type": rule_data["show_filter_type"],
            "show_ids": rule_data.get("show_ids"),
            "event_ids": rule_data.get("event_ids"),
            "seat_filter_type": rule_data["seat_filter_type"],
            "action_type": rule_data["action_type"],
            "action_value": rule_data.get("action_value"),
            "timing_type": rule_data["timing_type"],
            "timing_from_hours": rule_data.get("timing_from_hours"),
            "timing_to_hours": rule_data.get("timing_to_hours"),
            "is_active": rule_data.get("is_active", True),
            "notes": rule_data.get("notes"),
        }
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=500, detail="Failed to create rule")
        
        return dict[Any, Any](result)
    except HTTPException:
        raise
    except Exception as e:
        error_message = str(e)
        # Check if it's a duplicate rule violation (unique index on shows/events + action)
        if "idx_unique_rule" in error_message or "duplicate key" in error_message:
            raise HTTPException(
                status_code=409,
                detail="A rule with the same shows/events and action already exists. Please edit the existing rule or delete it first."
            )
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def update_rule_override(rule_id: str, rule_data: dict):
    """Update an existing rule override."""
    try:
        # Build the SET clause dynamically based on provided fields
        set_clauses = []
        values = {"rule_id": rule_id}
        
        for key, value in rule_data.items():
            if value is not None or key in ["show_ids", "event_ids", "action_value", "timing_value", "notes"]:
                set_clauses.append(f"{key} = :{key}")
                values[key] = value
        
        if not set_clauses:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        set_clauses.append("updated_at = CURRENT_TIMESTAMP")
        
        query = f"""
            UPDATE open_dist.listing_rule_override
            SET {', '.join(set_clauses)}
            WHERE id = :rule_id
            RETURNING *
        """
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        error_message = str(e)
        # Check if it's a duplicate rule violation (unique index on shows/events + action)
        if "idx_unique_rule" in error_message or "duplicate key" in error_message:
            raise HTTPException(
                status_code=409,
                detail="A rule with the same shows/events and action already exists. Please choose different values."
            )
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def delete_rule_override(rule_id: str):
    """Delete a rule override."""
    try:
        query = """
            DELETE FROM open_dist.listing_rule_override
            WHERE id = :rule_id
            RETURNING *
        """
        values = {"rule_id": rule_id}
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return {"message": "Rule deleted successfully", "rule": dict(result)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def activate_rule_override(rule_id: str):
    """Activate a rule override."""
    try:
        query = """
            UPDATE open_dist.listing_rule_override
            SET is_active = true, updated_at = CURRENT_TIMESTAMP
            WHERE id = :rule_id
            RETURNING *
        """
        values = {"rule_id": rule_id}
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def deactivate_rule_override(rule_id: str):
    """Deactivate a rule override."""
    try:
        query = """
            UPDATE open_dist.listing_rule_override
            SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE id = :rule_id
            RETURNING *
        """
        values = {"rule_id": rule_id}
        
        result = await get_pg_open_distribution_database().fetch_one(query=query, values=values)
        if result is None:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return dict(result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def reorder_rule_overrides(rule_orders: list):
    """Update priority_order for multiple rules efficiently, avoiding unique constraint conflicts."""
    try:
        # Build the values for the update
        rule_ids = []
        
        for rule_order in rule_orders:
            rule_id = str(rule_order["id"])
            rule_ids.append(f"'{rule_id}'")
        
        rule_ids_str = ", ".join(rule_ids)
        
        # Step 1: Set affected rules to negative values to clear conflicts
        query1 = f"""
            UPDATE open_dist.listing_rule_override
            SET priority_order = -priority_order
            WHERE id::text IN ({rule_ids_str}) AND priority_order > 0
        """
        await get_pg_open_distribution_database().execute(query=query1)
        
        # Step 2: Update to final priorities using CASE
        when_clauses = []
        for rule_order in rule_orders:
            rule_id = str(rule_order["id"])
            priority = rule_order["priority_order"]
            when_clauses.append(f"WHEN id = '{rule_id}'::uuid THEN {priority}")
        
        when_clause_str = "\n                ".join(when_clauses)
        
        query2 = f"""
            UPDATE open_dist.listing_rule_override
            SET 
                priority_order = CASE
                    {when_clause_str}
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id::text IN ({rule_ids_str})
        """
        await get_pg_open_distribution_database().execute(query=query2)
        
        return {"message": "Rules reordered successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_shows_for_dropdown():
    """Get all shows for dropdown selection."""
    try:
        query = """
            SELECT 
                id,
                title as show_name
            FROM open_dist.outbox_show
            ORDER BY title ASC
        """
        result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_events_for_dropdown(show_id=None):
    """Get events for dropdown selection, optionally filtered by show_id."""
    try:
        if show_id:
            query = """
                SELECT 
                    oe.id,
                    oe.title,
                    oe.event_start,
                    oe.show_id,
                    os.title as show_name,
                    osc.trade_desk_event_id
                FROM open_dist.outbox_event oe
                JOIN open_dist.outbox_show os ON oe.show_id = os.id
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                WHERE oe.show_id = :show_id
                AND oe.updated_at > current_timestamp - interval '24 hours'
                ORDER BY oe.event_start ASC
            """
            values = {"show_id": show_id}
            result = await get_pg_open_distribution_readonly_database().fetch_all(query=query, values=values)
        else:
            query = """
                SELECT 
                    oe.id,
                    oe.title,
                    oe.event_start,
                    oe.show_id,
                    os.title as show_name,
                    osc.trade_desk_event_id
                FROM open_dist.outbox_event oe
                JOIN open_dist.outbox_show os ON oe.show_id = os.id
                LEFT JOIN open_dist.outbox_sync_config osc ON osc.outbox_event_id = oe.id
                WHERE oe.updated_at > current_timestamp - interval '24 hours'
                ORDER BY oe.event_start ASC
                LIMIT 100
            """
            result = await get_pg_open_distribution_readonly_database().fetch_all(query=query)
        
        return [dict(row) for row in result]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def preview_rule_impact(rule_data: dict):
    """Calculate the potential impact of a rule."""
    try:
        # Build the WHERE clause based on filter type
        if rule_data["show_filter_type"] == "all":
            event_filter = "1=1"
            values = {}
        elif rule_data["show_filter_type"] == "specific_shows":
            event_filter = "oe.show_id = ANY(:show_ids::uuid[])"
            values = {"show_ids": rule_data.get("show_ids", [])}
        else:  # specific_events
            event_filter = "oe.id = ANY(:event_ids)"
            values = {"event_ids": rule_data.get("event_ids", [])}
        
        query = f"""
            WITH affected_events AS (
                SELECT DISTINCT
                    oe.id as event_id,
                    oe.show_id,
                    COUNT(DISTINCT ocl.id) as listing_count
                FROM open_dist.outbox_event oe
                LEFT JOIN open_dist.outbox_current_listing ocl ON ocl.event_id = oe.id
                WHERE {event_filter}
                GROUP BY oe.id, oe.show_id
            )
            SELECT 
                COUNT(DISTINCT show_id) as affected_shows_count,
                COUNT(DISTINCT event_id) as affected_events_count,
                COALESCE(SUM(listing_count), 0) as affected_listings_count
            FROM affected_events
        """
        
        result = await get_pg_open_distribution_readonly_database().fetch_one(query=query, values=values)
        
        # Generate impact description
        action_desc = {
            "put_up": "post listings for",
            "mark_override": "update markup for",
            "pull_down": "remove listings from"
        }.get(rule_data["action_type"], "affect")
        
        seat_desc = {
            "all": "all seats",
            "first_1_row": "first row in each section",
            "first_2_rows": "first 2 rows in each section",
            "first_3_rows": "first 3 rows in each section",
            "first_4_rows": "first 4 rows in each section"
        }.get(rule_data["seat_filter_type"], "seats")
        
        estimated_impact = f"This rule will {action_desc} {seat_desc} across {result['affected_events_count']} events"
        
        return {
            "affected_shows_count": result["affected_shows_count"],
            "affected_events_count": result["affected_events_count"],
            "affected_listings_count": result["affected_listings_count"],
            "estimated_impact": estimated_impact
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
