from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from app.auth.auth_system import get_current_user_with_roles
from app.db import open_distribution_db
from app.service.outbox_pdf_service import OutboxPdfService
from app.model.open_distribution_models import (
    EventDetailsModel,
    ShowDetailsModel,
    SyncActiveUpdateRequest,
    BulkUpdateRequest,
    UnmappedEventModel,
    TradeDeskEventModel,
    StubhubEventModel,
    MapEventRequest,
    MapStubhubEventRequest,
    RuleOverrideModel,
    RuleOverrideCreateRequest,
    RuleOverrideUpdateRequest,
    RuleReorderRequest,
    ShowDropdownModel,
    EventDropdownModel,
    RulePreviewRequest,
    RulePreviewResponse,
    OutboxPdfRequest,
    OutboxPdfResponse,
)
from app.model.user import User

router = APIRouter(prefix="/odis")


@router.get("/shows_all")
async def get_all_shows(
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_all_shows()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/shows", response_model=List[ShowDetailsModel])
async def get_shows_details(
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_shows_details()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/shows/{show_id}/events", response_model=List[EventDetailsModel])
async def get_shows_events(
        show_id: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_shows_events(show_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/shows/{show_id}/sync-active")
async def update_show_sync_active(
        show_id: str,
        request: SyncActiveUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Update sync_active for all events in a show (TradeDesk only)."""
    try:
        return await open_distribution_db.update_sync_active_by_show_id(
            show_id=show_id, sync_active=request.sync_active
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/shows/{show_id}/stubhub/sync-active")
async def update_stubhub_show_sync_active(
        show_id: str,
        request: SyncActiveUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Update sync_active for all events in a show (StubHub only)."""
    try:
        return await open_distribution_db.update_stubhub_sync_active_by_show_id(
            show_id=show_id, sync_active=request.sync_active
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/events/{outbox_event_id}/sync-active")
async def update_event_sync_active(
        outbox_event_id: str,
        request: SyncActiveUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Update sync_active for a specific event."""
    try:
        return await open_distribution_db.update_sync_active_by_event_id(
            outbox_event_id=outbox_event_id, sync_active=request.sync_active
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/events/bulk/update")
async def bulk_update_events(
        request: BulkUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Bulk update sync_active and/or markup_percent for multiple events."""
    try:
        return await open_distribution_db.bulk_update_by_event_ids(
            outbox_event_ids=request.outbox_event_ids,
            sync_active=request.sync_active,
            markup_percent=request.markup_percent,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/events/stubhub/bulk/update")
async def bulk_update_stubhub_events(
        request: BulkUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Bulk update sync_active and/or markup_percent for multiple StubHub events."""
    try:
        return await open_distribution_db.bulk_update_stubhub_by_event_ids(
            outbox_event_ids=request.outbox_event_ids,
            sync_active=request.sync_active,
            markup_percent=request.markup_percent,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/unmapped-events", response_model=List[UnmappedEventModel])
async def get_unmapped_events(
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_unmapped_events()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/trade-desk/search", response_model=List[TradeDeskEventModel])
async def get_trade_desk_events(
        event_date: str,
        show_name: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_trade_desk_events(event_date, show_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/stubhub/search", response_model=List[StubhubEventModel])
async def get_stubhub_events(
        event_date: str,
        show_name: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.get_stubhub_events(event_date, show_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/events/map")
async def map_events(
        request: MapEventRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.map_events(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/stubhub/map")
async def map_stubhub_events(
        request: MapStubhubEventRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    try:
        return await open_distribution_db.map_stubhub_events(request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/events/{event_id}/unmap")
async def unmap_event(
        event_id: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Unmap an event by deleting its TradeDesk sync configuration."""
    try:
        return await open_distribution_db.unmap_event(event_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/events/{event_id}/stubhub/unmap")
async def unmap_stubhub_event(
        event_id: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Unmap an event by deleting its StubHub sync configuration."""
    try:
        return await open_distribution_db.unmap_stubhub_event(event_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# =====================================================
# Rule Override Endpoints
# =====================================================

@router.get("/rules", response_model=List[RuleOverrideModel])
async def get_all_rules(
        is_active: Optional[bool] = Query(None, description="Filter by active status"),
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Get all rule overrides, optionally filtered by active status."""
    try:
        return await open_distribution_db.get_all_rule_overrides(is_active)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/rules/{rule_id}", response_model=RuleOverrideModel)
async def get_rule_by_id(
        rule_id: UUID,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Get a single rule override by ID."""
    try:
        return await open_distribution_db.get_rule_override_by_id(str(rule_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/rules", response_model=RuleOverrideModel)
async def create_rule(
        request: RuleOverrideCreateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Create a new rule override."""
    try:
        rule_data = request.model_dump(exclude_unset=True)
        return await open_distribution_db.create_rule_override(rule_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/rules/reorder")
async def reorder_rules(
        request: RuleReorderRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Update priority order for multiple rules."""
    try:
        return await open_distribution_db.reorder_rule_overrides(request.rule_orders)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/rules/{rule_id}", response_model=RuleOverrideModel)
async def update_rule(
        rule_id: UUID,
        request: RuleOverrideUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Update an existing rule override."""
    try:
        rule_data = request.model_dump(exclude_unset=True)
        return await open_distribution_db.update_rule_override(str(rule_id), rule_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/rules/{rule_id}")
async def delete_rule(
        rule_id: UUID,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Delete a rule override."""
    try:
        return await open_distribution_db.delete_rule_override(str(rule_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/rules/{rule_id}/activate", response_model=RuleOverrideModel)
async def activate_rule(
        rule_id: UUID,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Activate a rule override."""
    try:
        return await open_distribution_db.activate_rule_override(str(rule_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/rules/{rule_id}/deactivate", response_model=RuleOverrideModel)
async def deactivate_rule(
        rule_id: UUID,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Deactivate a rule override."""
    try:
        return await open_distribution_db.deactivate_rule_override(str(rule_id))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/rules/dropdowns/shows", response_model=List[ShowDropdownModel])
async def get_shows_dropdown(
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Get all shows for dropdown selection."""
    try:
        return await open_distribution_db.get_shows_for_dropdown()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/rules/dropdowns/events", response_model=List[EventDropdownModel])
async def get_events_dropdown(
        show_id: Optional[UUID] = Query(None, description="Filter events by show_id"),
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Get events for dropdown selection, optionally filtered by show_id."""
    try:
        show_id_str = str(show_id) if show_id else None
        return await open_distribution_db.get_events_for_dropdown(show_id_str)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/rules/preview", response_model=RulePreviewResponse)
async def preview_rule_impact(
        request: RulePreviewRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Calculate and preview the potential impact of a rule."""
    try:
        rule_data = request.model_dump(exclude_unset=True)
        return await open_distribution_db.preview_rule_impact(rule_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# =====================================================
# Outbox PDF Endpoints
# =====================================================

@router.post("/outbox-pdf/fetch", response_model=OutboxPdfResponse)
async def fetch_outbox_pdf(
        request: OutboxPdfRequest,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    """Fetch PDF ticket from Outbox by document_name."""
    try:
        service = OutboxPdfService()
        result = service.fetch_pdf_by_document_name(request.document_name)
        return OutboxPdfResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
