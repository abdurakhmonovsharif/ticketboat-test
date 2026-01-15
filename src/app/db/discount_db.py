import traceback
from typing import List, Dict, Any

from fastapi import HTTPException
from app.database import get_pg_buylist_database
from app.model.discount import CreateDiscountRequest, DiscountSerializer


async def create_discount(discount_data: CreateDiscountRequest, user_name: str) -> DiscountSerializer:
    """Create a new discount entry for a buylist item."""
    try:
        query = """
            INSERT INTO shadows_discount (buylist_id, event_code, performer_id, venue_id, discount_type, discount_text, created_by)
            VALUES (:buylist_id, :event_code, :performer_id, :venue_id, :discount_type, :discount_text, :created_by)
            RETURNING id, buylist_id, event_code, performer_id, venue_id, discount_type, discount_text, created_at, created_by
        """

        params = {
            "buylist_id": discount_data.buylist_id,
            "event_code": discount_data.event_code,
            "performer_id": discount_data.performer_id,
            "venue_id": discount_data.venue_id,
            "discount_type": discount_data.discount_type,
            "discount_text": discount_data.discount_text.strip(),
            "created_by": user_name
        }

        result = await get_pg_buylist_database().fetch_one(query, params)

        if not result:
            raise HTTPException(status_code=500, detail="Failed to create discount")

        return DiscountSerializer(**dict(result))

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while creating discount: {str(e)}"
        ) from e


async def get_discounts_by_buylist_id(buylist_id: str) -> List[DiscountSerializer]:
    """Get all applicable discounts for a specific buylist item based on discount_type."""
    try:
        # First get the buylist item details
        buylist_query = """
            SELECT id, venue_id, performer_id, event_code
            FROM shadows_buylist 
            WHERE id = :buylist_id
        """

        buylist_result = await get_pg_buylist_database().fetch_one(buylist_query, {"buylist_id": buylist_id})

        if not buylist_result:
            raise HTTPException(status_code=404, detail="Buylist item not found")

        buylist_data = dict(buylist_result)

        # Get all applicable discounts
        discount_query = """
            SELECT id, buylist_id, discount_text, discount_type, event_code, performer_id, venue_id, created_at, created_by
            FROM shadows_discount 
            WHERE event_code = :event_code
                OR performer_id = :performer_id 
                OR venue_id = :venue_id
            ORDER BY created_at ASC
        """

        params = {
            "event_code": buylist_data["event_code"],
            "performer_id": buylist_data["performer_id"],
            "venue_id": buylist_data["venue_id"]
        }

        results = await get_pg_buylist_database().fetch_all(discount_query, params)

        return [DiscountSerializer(**dict(result)) for result in results]

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while getting discounts: {str(e)}"
        ) from e


async def get_discounts_by_identifiers(event_code: str = None, performer_id: str = None, venue_id: str = None) -> List[DiscountSerializer]:
    """Get all applicable discounts based on event_code, performer_id, or venue_id identifiers."""
    try:
        if not event_code and not performer_id and not venue_id:
            raise HTTPException(status_code=400, detail="At least one identifier (event_code, performer_id, or venue_id) must be provided")

        # Build dynamic query based on provided identifiers
        conditions = []
        params = {}
        
        if event_code:
            conditions.append("event_code = :event_code")
            params["event_code"] = event_code
            
        if performer_id:
            conditions.append("performer_id = :performer_id")
            params["performer_id"] = performer_id
            
        if venue_id:
            conditions.append("venue_id = :venue_id")
            params["venue_id"] = venue_id

        discount_query = f"""
            SELECT id, buylist_id, discount_text, discount_type, event_code, performer_id, venue_id, created_at, created_by
            FROM shadows_discount 
            WHERE {' OR '.join(conditions)}
            ORDER BY created_at ASC
        """

        results = await get_pg_buylist_database().fetch_all(discount_query, params)

        return [DiscountSerializer(**dict(result)) for result in results]

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while getting discounts: {str(e)}"
        ) from e


async def delete_discount(discount_id: str) -> Dict[str, Any]:
    """Delete a discount entry."""
    try:
        # First check if discount exists and get buylist_id for logging
        check_query = "SELECT buylist_id FROM shadows_discount WHERE id = :id"
        existing = await get_pg_buylist_database().fetch_one(check_query, {"id": discount_id})

        if not existing:
            raise HTTPException(status_code=404, detail="Discount not found")

        # Delete the discount
        delete_query = "DELETE FROM shadows_discount WHERE id = :id RETURNING id"
        result = await get_pg_buylist_database().fetch_one(delete_query, {"id": discount_id})

        if not result:
            raise HTTPException(status_code=404, detail="Discount not found or already deleted")

        return {
            "status": "success",
            "message": "Discount deleted successfully",
            "discount_id": discount_id,
            "buylist_id": existing["buylist_id"]
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while deleting discount: {str(e)}"
        ) from e
