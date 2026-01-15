from fastapi import HTTPException

from app.database import get_pg_database
from app.model.cart_manager import AutoApprovePayload, UpdateAutoApprovePayload


async def create_cart_rules(payload: AutoApprovePayload, email: str):
    try:
        query = """
            INSERT INTO browser_data_capture.auto_approve_rules (
                event_name, venue, event_date_time, match_section, match_price, 
                rule_action, is_active, created_by, created_at, updated_at
            ) VALUES (
                :event_name, :venue, :event_date_time, :match_section, :match_price, 
                :rule_action, TRUE, :created_by, NOW(), NOW()
            )
            RETURNING rule_id, event_name, venue, event_date_time, match_section, match_price, rule_action, created_by;
        """

        values = {
            "event_name": payload.event_name,
            "venue": payload.venue,
            "event_date_time": payload.event_date_time,
            "match_section": payload.match_section,
            "match_price": payload.match_price,
            "rule_action": payload.rule_action,
            "created_by": email
        }

        result = await get_pg_database().fetch_one(query=query, values=values)

        return dict(result) if result else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def get_cart_rules(page: int, page_size: int):
    try:
        offset = (page - 1) * page_size

        query = """
            SELECT rule_id, event_name, venue, event_date_time, match_section, match_price, 
                   rule_action, is_active, created_by, created_at, updated_at
            FROM browser_data_capture.auto_approve_rules
            WHERE is_active = TRUE
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset;
        """

        count_query = """
            SELECT COUNT(*) FROM browser_data_capture.auto_approve_rules WHERE is_active = TRUE;
        """

        rules = await get_pg_database().fetch_all(query=query, values={"limit": page_size, "offset": offset})

        total = await get_pg_database().fetch_val(count_query)

        return {
            "items": [dict(row) for row in rules] if rules else [],
            "total": total
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


async def soft_delete_cart_rule(rule_id: int):
    """
    Sets is_active = FALSE for the given rule.
    Returns the rule data or None if not found.
    """
    try:
        query = """
            UPDATE browser_data_capture.auto_approve_rules
            SET is_active = FALSE,
                updated_at = NOW()
            WHERE rule_id = :rule_id
              AND is_active = TRUE
            RETURNING rule_id, event_name, venue, event_date_time, match_section,
                      match_price, rule_action, is_active, created_by, created_at, updated_at;
        """
        result = await get_pg_database().fetch_one(query=query, values={"rule_id": rule_id})
        return dict(result) if result else None
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting rule: {e}")


async def update_cart_rule(rule_id: int, payload: UpdateAutoApprovePayload):
    """
    Updates the given fields for the rule.
    Returns the updated row or None if no row was found.
    """
    try:
        query = """
            UPDATE browser_data_capture.auto_approve_rules
            SET
                event_name = :event_name,
                venue = :venue,
                event_date_time = :event_date_time,
                match_section = :match_section,
                match_price = :match_price,
                rule_action = :rule_action,
                is_active = COALESCE(:is_active, is_active),
                updated_at = NOW()
            WHERE rule_id = :rule_id
            RETURNING rule_id, event_name, venue, event_date_time, match_section,
                      match_price, rule_action, is_active, created_by, created_at, updated_at;
        """

        values = {
            "rule_id": rule_id,
            "event_name": payload.event_name,
            "venue": payload.venue,
            "event_date_time": payload.event_date_time,
            "match_section": payload.match_section,
            "match_price": payload.match_price,
            "rule_action": payload.rule_action,
            "is_active": payload.is_active,
        }
        result = await get_pg_database().fetch_one(query=query, values=values)
        return dict(result) if result else None

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating rule: {e}")
