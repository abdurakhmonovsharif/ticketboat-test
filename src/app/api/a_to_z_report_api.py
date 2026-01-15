from datetime import datetime
from typing import Optional, List, Dict

from fastapi import APIRouter, Depends, Query, HTTPException, Body

from app.auth.auth_system import get_current_user_with_roles, get_current_user
from app.db import a_to_z_report_db
from app.model.user import User
from app.model.a_to_z_report import (
    ReviewStatusItem,
    ReviewStatusInput,
    ReviewStatusRequest,
    EventCodesReviewStatusInput,
    CustomViewPayload,
    CustomViewResponse,
    DeleteCustomViewPayload
)

router = APIRouter(prefix="/a_to_z_report")


@router.get("/events")
async def event_reports(
    search_term: Optional[str] = Query(
        None, description="Search term to filter results."
    ),
    start_date: str = Query(default="", description="Start date"),
    end_date: str = Query(default="", description="End date"),
    days_to_sellout: Optional[int] = Query(default=None, description="Days to sellout"),
    sellout_confidence_min: Optional[float] = Query(
        default=None, description="Minimum Sellout confidence"
    ),
    sellout_confidence_max: Optional[float] = Query(
        default=None, description="Maximum Sellout confidence"
    ),
    weekend_only: Optional[bool] = Query(
        default=None, description="Show only weekends"
    ),
    weekdays_only: Optional[bool] = Query(
        default=None, description="Show only weekdays"
    ),
    days_to_show_min: Optional[int] = Query(default=None, description="Days to show"),
    days_to_show_max: Optional[int] = Query(default=None, description="Days to show"),
    projected_margin_min: Optional[float] = Query(
        default=None, description="Minimum projected margin"
    ),
    projected_margin_max: Optional[float] = Query(
        default=None, description="Maximum projected margin"
    ),
    velocity_min: Optional[float] = Query(default=None, description="Minimum velocity"),
    velocity_max: Optional[float] = Query(default=None, description="Maximum velocity"),
    #new fields
    tickets_available_primary_min: Optional[int] = Query(
        default=None, description="Minimum tickets available primary"
    ),
    tickets_available_primary_max: Optional[int] = Query(
        default=None, description="Maximum tickets available primary"
    ),
    tickets_available_secondary_min: Optional[int] = Query(
        default=None, description="Minimum tickets available secondary"
    ),
    tickets_available_secondary_max: Optional[int] = Query(
        default=None, description="Maximum tickets available secondary"
    ),
    get_in_primary_tickets_min: Optional[int] = Query(
        default=None, description="Minimum get in primary tickets"
    ),
    get_in_primary_tickets_max: Optional[int] = Query(
        default=None, description="Maximum get in primary tickets"
    ),
    get_in_primary_min: Optional[float] = Query(
        default=None, description="Minimum get in primary"
    ),
    get_in_primary_max: Optional[float] = Query(
        default=None, description="Maximum get in primary"
    ),
    get_in_secondary_min: Optional[float] = Query(
        default=None, description="Minimum get in secondary"
    ),
    get_in_secondary_max: Optional[float] = Query(
        default=None, description="Maximum get in secondary"
    ),
    percent_inventory_currently_available_min: Optional[float] = Query(
        default=None, description="Minimum percentage inventory currently available"
    ),
    percent_inventory_currently_available_max: Optional[float] = Query(
        default=None, description="Maximum percentage inventory currently available"
    ),
    seat_geek_velocity_min: Optional[float] = Query(
        default=None, description="Minimum Seat Geek velocity"
    ),
    seat_geek_velocity_max: Optional[float] = Query(
        default=None, description="Maximum Seat Geek velocity"
    ),
    stubhub_velocity_min: Optional[float] = Query(
        default=None, description="Minimum StubHub velocity"
    ),
    stubhub_velocity_max: Optional[float] = Query(
        default=None, description="Maximum StubHub velocity"
    ),
    
    sort_by: str = Query(
        default="start_date",
        description="Field to sort by",
    ),
    sort_order: str = Query(
        default="desc", description="Sort order: asc or desc", regex="^(asc|desc)$"
    ),
    page_size: int = Query(
        default=50, description="Number of results to return per page"
    ),
    page: int = Query(default=1, description="Page number to return"),
    user: User = Depends(get_current_user_with_roles(["user"])),
):
    try:
        return await a_to_z_report_db.get_a_to_z_report_overview(
            search_term,
            page,
            page_size,
            start_date,
            end_date,
            days_to_sellout,
            sellout_confidence_min,
            sellout_confidence_max,
            weekend_only,
            weekdays_only,
            days_to_show_min,
            days_to_show_max,
            projected_margin_min,
            projected_margin_max,
            velocity_min,
            velocity_max,
            tickets_available_primary_min,
            tickets_available_primary_max,
            tickets_available_secondary_min,
            tickets_available_secondary_max,
            get_in_primary_tickets_min,
            get_in_primary_tickets_max,
            get_in_primary_min,
            get_in_primary_max,
            get_in_secondary_min,
            get_in_secondary_max,
            percent_inventory_currently_available_min,
            percent_inventory_currently_available_max,
            seat_geek_velocity_min,
            seat_geek_velocity_max,
            stubhub_velocity_min,
            stubhub_velocity_max,
            sort_by,
            sort_order,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/primary-event-stats")
async def get_primary_event_stats(
    event_code: str = Query(..., description="Event code to get stats for")
):
    try:
        return await a_to_z_report_db.get_primary_event_stats(event_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/secondary-event-stats")
async def get_secondary_event_stats(
    event_code: str = Query(..., description="Event code to get stats for")
):
    try:
        return await a_to_z_report_db.get_secondary_event_stats(event_code)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

@router.post("/get-review-status", response_model=List[ReviewStatusItem])
async def api_get_review_status(
    payload: ReviewStatusRequest
):
    try:
        return await a_to_z_report_db.get_review_status(payload.items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.post("/post-review-status")
async def api_post_review_status(
    payload: ReviewStatusInput,
    user: User = Depends(get_current_user())
):
    try:
        payload_with_user = payload.model_copy(update={"reviewed_by": user.name})
        return await a_to_z_report_db.post_review_status(payload_with_user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.post("/get-event-codes-by-review-status")
async def api_get_event_codes_review_status(
    payload: EventCodesReviewStatusInput
):
    try:
        return await a_to_z_report_db.get_event_codes_review_status(
            payload.review_status,
            payload.page_size,
            payload.page
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.post("/get-events-with-review-status")
async def api_get_events_with_review_status(
    payload: ReviewStatusRequest
):
    try:
        return await a_to_z_report_db.api_get_events_with_review_status(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

@router.get("/get-section-mapping")
async def api_get_section_mapping(
    search_term: Optional[str] = Query(
        None, description="Search term to filter results."
    ),
    tm_event_code: str = Query(default="", description="Ticketmaster event code"),
    td_event_id: str = Query(default="", description="Tradedesk event id"),
    start_date: str = Query(default="", description="Start date"),
    end_date: str = Query(default="", description="End date"),
    tm_section: str = Query(default="", description="Ticketmaster Section"),
    td_section: str = Query(default="", description="Tradedesk Section"),
    tm_quantity_min: Optional[int] = Query(
        default=None, description="Minimum Ticketmaster quantity"
    ),
    tm_quantity_max: Optional[int] = Query(
        default=None, description="Maximum Ticketmaster quantity"
    ),
    td_quantity_min: Optional[int] = Query(
        default=None, description="Minimum Tradedesk quantity"
    ),
    td_quantity_max: Optional[int] = Query(
        default=None, description="Maximum Tradedesk quantity"
    ),
    tm_section_capacity_min: Optional[int] = Query(
        default=None, description="Minimum Ticketmaster section capacity"
    ),
    tm_section_capacity_max: Optional[int] = Query(
        default=None, description="Maximum Ticketmaster section capacity"
    ),
    td_section_capacity_min: Optional[int] = Query(
        default=None, description="Minimum Tradedesk section capacity"
    ),
    td_section_capacity_max: Optional[int] = Query(
        default=None, description="Maximum Tradedesk section capacity"
    ),
    tm_percent_remaining_section_min: Optional[float] = Query(
        default=None, description="Minimum Ticketmaster percent remaining per section"
    ),
    tm_percent_remaining_section_max: Optional[float] = Query(
        default=None, description="Maximum Ticketmaster percent remaining per section"
    ),
    td_percent_remaining_section_min: Optional[float] = Query(
        default=None, description="Minimum Tradedesk percent remaining per section"
    ),
    td_percent_remaining_section_max: Optional[float] = Query(
        default=None, description="Maximum Tradedesk percent remaining per section"
    ),
    tm_total_quantity_min: Optional[int] = Query(
        default=None, description="Minimum Ticketmaster total quantity"
    ),
    tm_total_quantity_max: Optional[int] = Query(
        default=None, description="Maximum Ticketmaster total quantity"
    ),
    tm_total_capacity_min: Optional[int] = Query(
        default=None, description="Minimum Ticketmaster total capacity"
    ),
    tm_total_capacity_max: Optional[int] = Query(
        default=None, description="Maximum Ticketmaster total capacity"
    ),
    tm_total_percent_remaining_min: Optional[float] = Query(
        default=None, description="Minimum Ticketmaster total percent remaining per event"
    ),
    tm_total_percent_remaining_max: Optional[float] = Query(
        default=None, description="Maximum Ticketmaster total percent remaining per event"
    ),
    tm_section_getin_min: Optional[float] = Query(
        default=None, description="Minimum Ticketmaster section getin"
    ),
    tm_section_getin_max: Optional[float] = Query(
        default=None, description="Maximum Ticketmaster section getin"
    ),
    td_section_getin_min: Optional[float] = Query(
        default=None, description="Minimum Tradedesk section getin"
    ),
    td_section_getin_max: Optional[float] = Query(
        default=None, description="Maximum Tradedesk section getin"
    ),
    tm_section_has_resale: Optional[str] = Query(
        default=None, description="Ticketmaster section has_resale"
    ),
    predicted_section_sellout_start_date: str = Query(
        default="", description="Predicted section sellout start date"
    ),
    predicted_section_sellout_end_date: str = Query(
        default="", description="Predicted section sellout end date"
    ),
    days_to_sellout_min: Optional[int] = Query(
        default=None, description="Minimum Days to sellout date"
    ),
    days_to_sellout_max: Optional[int] = Query(
        default=None, description="Maximum Days to sellout date"
    ),
    section_sellout_confidence_min: Optional[float] = Query(
        default=None, description="Minimum Section sellout confidence"
    ),
    section_sellout_confidence_max: Optional[float] = Query(
        default=None, description="Maximum Section sellout confidence"
    ),
    section_velocity_min: Optional[float] = Query(default=None, description="Minimum section velocity"),
    section_velocity_max: Optional[float] = Query(default=None, description="Maximum section velocity"),
    source_name: str = Query(default="", description="Secondary source name"),
    sort_by: str = Query(
        default="start_date",
        description="Field to sort by",
    ),
    sort_order: str = Query(
        default="desc", description="Sort order: asc or desc", regex="^(asc|desc)$"
    ),
    page_size: int = Query(
        default=50, description="Number of results to return per page"
    ),
    page: int = Query(default=1, description="Page number to return"),
    user: User = Depends(get_current_user_with_roles(["user"])),
    review_event_codes: Optional[List[str]] = Query(default=[])
):
    try:
        return await a_to_z_report_db.get_section_mapping(
            search_term,
            page,
            page_size,
            start_date,
            end_date,
            tm_event_code,
            td_event_id,
            tm_section,
            td_section,
            tm_quantity_min,
            tm_quantity_max,
            td_quantity_min,
            td_quantity_max,
            tm_section_capacity_min,
            tm_section_capacity_max,
            td_section_capacity_min,
            td_section_capacity_max,
            tm_percent_remaining_section_min,
            tm_percent_remaining_section_max,
            td_percent_remaining_section_min,
            td_percent_remaining_section_max,
            tm_total_quantity_min,
            tm_total_quantity_max,
            tm_total_capacity_min,
            tm_total_capacity_max,
            tm_total_percent_remaining_min,
            tm_total_percent_remaining_max,
            tm_section_getin_min,
            tm_section_getin_max,
            td_section_getin_min,
            td_section_getin_max,
            tm_section_has_resale,
            predicted_section_sellout_start_date,
            predicted_section_sellout_end_date,
            days_to_sellout_min,
            days_to_sellout_max,
            section_sellout_confidence_min,
            section_sellout_confidence_max,
            section_velocity_min,
            section_velocity_max,
            source_name,
            sort_by,
            sort_order,
            review_event_codes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")

@router.post("/save-custom-view", response_model=CustomViewResponse)
async def save_custom_view(payload: CustomViewPayload):
    try:
        return await a_to_z_report_db.insert_custom_view(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving custom view: {e}")

@router.put("/update-custom-view/{view_id}", response_model=CustomViewResponse)
async def update_custom_view(view_id: str, payload: CustomViewPayload):
    try:
        return await a_to_z_report_db.update_custom_view(view_id, payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating custom view: {e}")

@router.get("/get-all-custom-views")
async def get_all_custom_views():
    return await a_to_z_report_db.get_all_custom_views()

@router.delete("/delete-custom-view")
async def delete_custom_view(payload: DeleteCustomViewPayload) -> Dict[str, str]:
    try:
        return await a_to_z_report_db.delete_custom_view(payload)
    except Exception as e:
        print(f"Error deleting custom view: {e}")
        raise HTTPException(status_code=500, detail="An error occurred while deleting the custom view.")


@router.get("/get-price-break")
async def api_get_price_break(
    search_term: Optional[str] = Query(
        None, description="Search term to filter results."
    ),
    event_code: str = Query(default="", description="Ticketmaster event code"),
    event_name: str = Query(default="", description="Ticketmaster event name"),
    section: str = Query(default="", description="Seating Section"),
    offer_code: str = Query(default="", description="Offer Code"),
    venue: str = Query(default="", description="Venue Name"),
    city: str = Query(default="", description="City"),
    start_date: str = Query(default="", description="Start date"),
    end_date: str = Query(default="", description="End date"),
    td_price_bracket: str = Query(default="", description="Tradedesk Price Bracket, e.g.: $51-$100"),
    tm_quantity_min: Optional[int] = Query(
        default=None, description="Minimum Ticketmaster quantity"
    ),
    tm_quantity_max: Optional[int] = Query(
        default=None, description="Maximum Ticketmaster quantity"
    ),
    td_quantity_min: Optional[int] = Query(
        default=None, description="Minimum Tradedesk quantity"
    ),
    td_quantity_max: Optional[int] = Query(
        default=None, description="Maximum Tradedesk quantity"
    ),
    total_price_min: Optional[float] = Query(
        default=None, description="Minimum Ticketmaster total price"
    ),
    total_price_max: Optional[float] = Query(
        default=None, description="Maximum Ticketmaster total price"
    ),
    offer_predicted_sellout_start_date: str = Query(
        default="", description="Predicted offer sellout start date"
    ),
    offer_predicted_sellout_end_date: str = Query(
        default="", description="Predicted offer sellout end date"
    ),
    offer_sellout_confidence_min: Optional[float] = Query(
        default=None, description="Minimum Offer sellout confidence"
    ),
    offer_sellout_confidence_max: Optional[float] = Query(
        default=None, description="Maximum Offer sellout confidence"
    ),
    predicted_velocity_min: Optional[float] = Query(
        default=None, description="Minimum predicted velocity"
    ),
    predicted_velocity_max: Optional[float] = Query(
        default=None, description="Maximum predicted velocity"
    ),
    days_to_sellout_min: Optional[int] = Query(
        default=None, description="Minimum Days to sellout date"
    ),
    days_to_sellout_max: Optional[int] = Query(
        default=None, description="Maximum Days to sellout date"
    ),
    percent_tickets_remaining_min: Optional[float] = Query(
        default=None, description="Minimum Percent Tickets Remaining"
    ),
    percent_tickets_remaining_max: Optional[float] = Query(
        default=None, description="Maximum Percent Tickets Remaining"
    ),
    sort_by: str = Query(
        default="start_date",
        description="Field to sort by",
    ),
    sort_order: str = Query(
        default="desc", description="Sort order: asc or desc", regex="^(asc|desc)$"
    ),
    page_size: int = Query(
        default=50, description="Number of results to return per page"
    ),
    page: int = Query(default=1, description="Page number to return"),
    user: User = Depends(get_current_user_with_roles(["user"])),
    review_event_codes: Optional[List[str]] = Query(default=[])
):
    try:
        return await a_to_z_report_db.get_price_break(
            search_term,
            page,
            page_size,
            event_code,
            event_name,
            section,
            offer_code,
            venue,
            city,
            start_date,
            end_date,
            td_price_bracket,
            tm_quantity_min,
            tm_quantity_max,
            td_quantity_min,
            td_quantity_max,
            total_price_min,
            total_price_max,
            offer_predicted_sellout_start_date,
            offer_predicted_sellout_end_date,
            offer_sellout_confidence_min,
            offer_sellout_confidence_max,
            predicted_velocity_min,
            predicted_velocity_max,
            days_to_sellout_min,
            days_to_sellout_max,
            percent_tickets_remaining_min,
            percent_tickets_remaining_max,
            sort_by,
            sort_order,
            review_event_codes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
        
        
@router.post("/post-pricebreak-review-status")
async def api_post_pricebreak_review_status(
    payload: dict,
    user: User = Depends(get_current_user())
):
    try:
        payload_with_user = {**payload, "reviewed_by": user.name}

        return await a_to_z_report_db.post_pricebreak_review_status(payload_with_user)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")
