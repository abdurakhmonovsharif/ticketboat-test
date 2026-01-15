import traceback
from typing import Optional, List

from fastapi import APIRouter, Query, Depends, HTTPException
from starlette import status

from app.db import onsale_email_analysis_db
from app.auth.auth_system import get_current_user_with_roles
from app.model.onsale_email_analysis import (
    OnsaleEmailAnalysisResponse,
    OnsaleEmailAnalysisSummary,
    FilterOptionsResponse
)
from app.model.user import User
from app.service.email_service import EmailService

router = APIRouter(prefix="/reports/emails/onsale-analysis")


@router.get("", response_model=OnsaleEmailAnalysisResponse)
async def get_onsale_email_analyses(
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        page_size: int = Query(
            default=20,
            description="Number of results to return per page",
        ),
        search_term: Optional[str] = Query(
            default=None,
            description="Search term for events, performers, venues",
        ),
        venue: Optional[List[str]] = Query(
            default=None,
            description="Filter by venue names",
        ),
        performer: Optional[List[str]] = Query(
            default=None,
            description="Filter by performer names",
        ),
        event_date_start: Optional[str] = Query(
            default=None,
            description="Start date for event date filtering (YYYY-MM-DD)",
        ),
        event_date_end: Optional[str] = Query(
            default=None,
            description="End date for event date filtering (YYYY-MM-DD)",
        ),
        onsale_date_start: Optional[str] = Query(
            default=None,
            description="Start date for onsale date filtering (YYYY-MM-DD)",
        ),
        onsale_date_end: Optional[str] = Query(
            default=None,
            description="End date for onsale date filtering (YYYY-MM-DD)",
        ),
        presale_date_start: Optional[str] = Query(
            default=None,
            description="Start date for presale date filtering (YYYY-MM-DD)",
        ),
        presale_date_end: Optional[str] = Query(
            default=None,
            description="End date for presale date filtering (YYYY-MM-DD)",
        ),
        event_type: Optional[str] = Query(
            default=None,
            description="Filter by event type",
        ),
        market_volatility_level: Optional[str] = Query(
            default=None,
            description="Filter by market volatility level",
        ),
        demand_uncertainty_level: Optional[str] = Query(
            default=None,
            description="Filter by demand uncertainty level",
        ),
        competition_level: Optional[str] = Query(
            default=None,
            description="Filter by competition level",
        ),
        overall_opportunity_level: Optional[str] = Query(
            default=None,
            description="Filter by Overall Opportunity level (hot, great, good)",
        ),
        min_estimated_profit: Optional[float] = Query(
            default=None,
            description="Minimum estimated total profit in USD",
        ),
        sort_field: Optional[str] = Query(
            default=None,
            description="Field to sort by (overallOpportunity, buyability, estimated_profit, event_date, email_ts)",
        ),
        sort_order: Optional[str] = Query(
            default=None,
            description="Sort order (ascend or descend)",
        ),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> OnsaleEmailAnalysisResponse:
    """
    Get paginated onsale email analysis data with filters.
    """
    try:
        result = await onsale_email_analysis_db.get_onsale_email_analyses(
            page=page,
            page_size=page_size,
            search_term=search_term,
            venue=venue,
            performer=performer,
            event_date_start=event_date_start,
            event_date_end=event_date_end,
            onsale_date_start=onsale_date_start,
            onsale_date_end=onsale_date_end,
            presale_date_start=presale_date_start,
            presale_date_end=presale_date_end,
            event_type=event_type,
            market_volatility_level=market_volatility_level,
            demand_uncertainty_level=demand_uncertainty_level,
            competition_level=competition_level,
            overall_opportunity_level=overall_opportunity_level,
            min_estimated_profit=min_estimated_profit,
            sort_field=sort_field,
            sort_order=sort_order,
            timezone=timezone
        )
        return OnsaleEmailAnalysisResponse(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching onsale email analyses: {str(e)}"
        )


@router.get("/summary", response_model=OnsaleEmailAnalysisSummary)
async def get_onsale_email_analysis_summary(
        search_term: Optional[str] = Query(
            default=None,
            description="Search term for events, performers, venues",
        ),
        venue: Optional[List[str]] = Query(
            default=None,
            description="Filter by venue names",
        ),
        performer: Optional[List[str]] = Query(
            default=None,
            description="Filter by performer names",
        ),
        event_date_start: Optional[str] = Query(
            default=None,
            description="Start date for event date filtering (YYYY-MM-DD)",
        ),
        event_date_end: Optional[str] = Query(
            default=None,
            description="End date for event date filtering (YYYY-MM-DD)",
        ),
        onsale_date_start: Optional[str] = Query(
            default=None,
            description="Start date for onsale date filtering (YYYY-MM-DD)",
        ),
        onsale_date_end: Optional[str] = Query(
            default=None,
            description="End date for onsale date filtering (YYYY-MM-DD)",
        ),
        presale_date_start: Optional[str] = Query(
            default=None,
            description="Start date for presale date filtering (YYYY-MM-DD)",
        ),
        presale_date_end: Optional[str] = Query(
            default=None,
            description="End date for presale date filtering (YYYY-MM-DD)",
        ),
        event_type: Optional[str] = Query(
            default=None,
            description="Filter by event type",
        ),
        market_volatility_level: Optional[str] = Query(
            default=None,
            description="Filter by market volatility level",
        ),
        demand_uncertainty_level: Optional[str] = Query(
            default=None,
            description="Filter by demand uncertainty level",
        ),
        competition_level: Optional[str] = Query(
            default=None,
            description="Filter by competition level",
        ),
        overall_opportunity_level: Optional[str] = Query(
            default=None,
            description="Filter by Overall Opportunity level (hot, great, good)",
        ),
        min_estimated_profit: Optional[float] = Query(
            default=None,
            description="Minimum estimated total profit in USD",
        ),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> OnsaleEmailAnalysisSummary:
    """
    Get summary statistics for onsale email analysis.
    """
    try:
        result = await onsale_email_analysis_db.get_onsale_email_analysis_summary(
            search_term=search_term,
            venue=venue,
            performer=performer,
            event_date_start=event_date_start,
            event_date_end=event_date_end,
            onsale_date_start=onsale_date_start,
            onsale_date_end=onsale_date_end,
            presale_date_start=presale_date_start,
            presale_date_end=presale_date_end,
            event_type=event_type,
            market_volatility_level=market_volatility_level,
            demand_uncertainty_level=demand_uncertainty_level,
            competition_level=competition_level,
            overall_opportunity_level=overall_opportunity_level,
            min_estimated_profit=min_estimated_profit,
            timezone=timezone
        )
        return OnsaleEmailAnalysisSummary(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching summary: {str(e)}"
        )


@router.get("/venues", response_model=FilterOptionsResponse)
async def get_onsale_email_analysis_venues(
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> FilterOptionsResponse:
    """
    Get unique venues from onsale email analysis.
    """
    try:
        result = await onsale_email_analysis_db.get_onsale_email_analysis_venues()
        return FilterOptionsResponse(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching venues: {str(e)}"
        )


@router.get("/performers", response_model=FilterOptionsResponse)
async def get_onsale_email_analysis_performers(
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> FilterOptionsResponse:
    """
    Get unique performers from onsale email analysis.
    """
    try:
        result = await onsale_email_analysis_db.get_onsale_email_analysis_performers()
        return FilterOptionsResponse(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching performers: {str(e)}"
        )


@router.get("/event-types", response_model=FilterOptionsResponse)
async def get_onsale_email_analysis_event_types(
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> FilterOptionsResponse:
    """
    Get unique event types from onsale email analysis.
    """
    try:
        result = await onsale_email_analysis_db.get_onsale_email_analysis_event_types()
        return FilterOptionsResponse(**result)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching event types: {str(e)}"
        )


@router.get("/email/{deduplication_id}")
async def get_email_by_deduplication_id(
        deduplication_id: str,
        from_email: Optional[str] = Query(
            default=None,
            description="From email address to help find the correct email",
        ),
        to_email: Optional[str] = Query(
            default=None,
            description="To email address to help find the correct email",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> str:
    """
    Get email content by deduplication_id using OpenSearch.
    """
    try:
        email_service = EmailService()
        email_content = await email_service.get_email_by_deduplication_id(
            deduplication_id, from_email, to_email
        )
        return email_content
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching email content: {str(e)}"
        )
