from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import purchase_tracking_db, event_report_db, buyer_report_db
from app.model.user import User

router = APIRouter(prefix="/purchase_tracking")


@router.get("/recent_purchases")
async def recent_purchases(
        search_term: Optional[str] = Query("", description="Search term to filter results"),
        marketplace: Optional[str] = Query("", description="Filter by marketplace (Ticketmaster or eTix)"),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        purchase_start_date: str = Query(
            default="",
            description="Start date"
        ),
        purchase_end_date: str = Query(
            default="",
            description="End date"
        ),
        event_start_date: str = Query(
            default="",
            description="Start date"
        ),
        event_end_date: str = Query(
            default="",
            description="End date"
        ),
        company: str = Query(
            default="",
            description="Filter by company name"
        ),
        statuses: Optional[str] = Query(
            default=None,
            description="Comma-separated statuses to filter results (e.g., 'pending,matched,unmatched')"
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await purchase_tracking_db.get_recent_purchases(
        timezone,
        search_term,
        marketplace,
        page,
        page_size,
        purchase_start_date,
        purchase_end_date,
        event_start_date,
        event_end_date,
        statuses,
        company,
    )


@router.get("/primary_issues")
async def primary_issues(
    account: Optional[str] = Query(None, description="Filter by account"),
    start_date: Optional[str] = Query(None, description="Start date"),
    end_date: Optional[str] = Query(None, description="End date"),
    error: Optional[str] = Query(None, description="Filter by error"),
    primary: Optional[str] = Query(None, description="Filter by primary"),
    sort: Optional[str] = Query(None, description="Sort order"),
    page: Optional[int] = Query(None, description="Page number"),
    limit: Optional[int] = Query(None, description="Page size"),
    _user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await purchase_tracking_db.get_primary_issues(
        start_date=start_date,
        end_date=end_date,
        account=account,
        error=error,
        primary=primary,
        sort=sort,
        page=page,
        limit=limit,
    )

@router.get("/primary_issues/grouped_by_account")
async def primary_issues_by_account(
    start_date: Optional[str] = Query(None, description="Start date"),
    end_date: Optional[str] = Query(None, description="End date"),
    account: Optional[str] = Query(None, description="Filter by account"),
    error: Optional[str] = Query(None, description="Filter by error"),
    primary: Optional[str] = Query(None, description="Filter by primary"),
    sort: Optional[str] = Query(None, description="Sort order"),
    page: Optional[int] = Query(None, description="Page number"),
    limit: Optional[int] = Query(None, description="Page size"),
    _user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await purchase_tracking_db.get_primary_issues_grouped_by_account(
        start_date=start_date,
        end_date=end_date,
        account=account,
        error=error,
        primary=primary,
        sort=sort,
        page=page,
        limit=limit,
    )


@router.get("/recent_purchases/export")
async def recent_purchases_export(
        search_term: Optional[str] = Query(None, description="Search term to filter results."),
        marketplace: Optional[str] = Query(None, description="Filter by marketplace (Ticketmaster or eTix)."),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        purchase_start_date: str = Query(
            default="",
            description="Start date"
        ),
        purchase_end_date: str = Query(
            default="",
            description="End date"
        ),
        event_start_date: str = Query(
            default="",
            description="Start date"
        ),
        event_end_date: str = Query(
            default="",
            description="End date"
        ),
        statuses: Optional[str] = Query(
            default=None,
            description="Comma-separated statuses to filter results (e.g., 'pending,matched,unmatched')"
        ),
        company: str = Query(
            default="",
            description="Filter by company name"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    result = await purchase_tracking_db.get_recent_purchases(
        timezone,
        search_term,
        marketplace,
        purchase_start_date=purchase_start_date,
        purchase_end_date=purchase_end_date,
        event_start_date=event_start_date,
        event_end_date=event_end_date,
        statuses=statuses,
        company=company,
    )
    data = [
        {
            'Date':
                item['created'].strftime("%m/%d/%Y %I:%M:%S %p")
                if isinstance(item['created'], datetime)
                else '',
            'Buyer Email': item.get('email', ''),
            'Multilogin Profile': item.get('multilogin_profile', ''),
            'Event Date':
                item['event_date_local'].strftime("%m/%d/%Y %I:%M:%S %p")
                if isinstance(item['event_date_local'], datetime)
                else '',
            'Event Name': item.get('event_name', ''),
            'Venue': item.get('venue', ''),
            'Event URL': item.get('url', ''),
            'Order Number': item.get('order_number', '')
        }
        for item in result['items']
    ]

    headers = [
        'Date', 'Buyer Email', 'Multilogin Profile', 'Event Date', 'Event Name', 'Venue', 'Event URL', 'Order Number'
    ]
    return purchase_tracking_db.generate_csv_response(data, headers, "recent_purchases")


@router.get("/buyer_report")
async def buyer_reports(
        search_term: Optional[str] = Query(None, description="Search term to filter results."),
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            None,
            description="Start hour (0-23) for current day filtering",
            ge=0,
            le=23
        ),
        end_hour: Optional[int] = Query(
            None,
            description="End hour (0-23) for current day filtering",
            ge=0,
            le=23
        ),
        sort_by: str = Query(
            default="total_quantity",
            description="Field to sort by: email, total_quantity, or total_cost"
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: asc or desc",
            regex="^(asc|desc)$"
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page"
        ),
        page: int = Query(
            default=1,
            description="Page number to return"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await buyer_report_db.get_buyer_reports(
        search_term,
        start_date,
        end_date,
        start_hour,
        end_hour,
        page_size,
        page,
        sort_by,
        sort_order
    )


@router.get("/event_report")
async def event_reports(
        search_term: Optional[str] = Query(None, description="Search term to filter results."),
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            None,
            description="Start hour (0-23) for current day filtering",
            ge=0,
            le=23
        ),
        end_hour: Optional[int] = Query(
            None,
            description="End hour (0-23) for current day filtering",
            ge=0,
            le=23
        ),
        sort_by: str = Query(
            default="total_quantity",
            description="Field to sort by: email, total_quantity, or total_cost"
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: asc or desc",
            regex="^(asc|desc)$"
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page"
        ),
        page: int = Query(
            default=1,
            description="Page number to return"
        ),
        data_type: Optional[str] = Query(
            default='event',
            description="Type of the result data"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await event_report_db.get_event_reports(
            search_term,
            start_date,
            end_date,
            start_hour,
            end_hour,
            page_size,
            page,
            sort_by,
            sort_order,
            data_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/buyer_report/export")
async def export_buyer_reports(
        search_term: Optional[str] = Query(None, description="Search term to filter results."),
        start_date: str = Query(default="", description="Start date"),
        end_date: str = Query(default="", description="End date"),
        start_hour: Optional[int] = Query(None, description="Start hour (0-23)"),
        end_hour: Optional[int] = Query(None, description="End hour (0-23)"),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    results = await buyer_report_db.get_buyer_reports_detail(
        search_term=search_term,
        start_date=start_date,
        end_date=end_date,
        start_hour=start_hour,
        end_hour=end_hour
    )

    data = [
        {
            'Source': item['source'],
            'Created': item['created'],
            'Email': item['email'],
            'Order Number': item['order_number'],
            'Event ID': item['event_id'],
            'Event Name': item['event_name'],
            'Event Date': item['event_date_local'],
            'URL': item['url'],
            'Venue': item['venue'],
            'City': item['city'],
            'Region': item['region'],
            'Country': item['country'],
            'Order Fee': f"${item['order_fee']:.2f}" if item['order_fee'] else None,
            'Section': item['section'],
            'Row': item['row'],
            'Seat Names': item['seat_names'],
            'Is General Admission': item['is_general_admission'],
            'Quantity': item['quantity'],
            'Price Per Ticket': f"${item['price_per_ticket']:.2f}" if item['price_per_ticket'] else None,
            'Taxes Per Ticket': f"${item['taxes_per_ticket']:.2f}" if item['taxes_per_ticket'] else None,
            'Service Charges Per Ticket': f"${item['service_charges_per_ticket']:.2f}" if item[
                'service_charges_per_ticket'] else None,
            'Facility Charges Per Ticket': f"${item['facility_charges_per_ticket']:.2f}" if item[
                'facility_charges_per_ticket'] else None,
            'Total Price': f"${item['total_price']:.2f}" if item[
                'total_price'] else None,
            'Total Profit': f"${item['total_profit']:.2f}" if item[
                'total_profit'] else None,
            'Margin': f"${item['margin']:.2f}" if item[
                'margin'] else None
        }
        for item in results
    ]

    headers = [
        'Source', 'Created', 'Email', 'Order Number', 'Event ID', 'Event Name', 'Event Date', 'URL',
        'Venue', 'City', 'Region', 'Country', 'Order Fee', 'Section', 'Row', 'Seat Names',
        'Is General Admission', 'Quantity', 'Price Per Ticket', 'Taxes Per Ticket',
        'Service Charges Per Ticket', 'Facility Charges Per Ticket', 'Total Price', 'Total Profit', 'Margin'
    ]

    return purchase_tracking_db.generate_csv_response(data, headers, "buyer_report")


@router.get("/purchase_report_by_email")
async def purchase_report_by_email(
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            default=None,
            description="Start hour (0-23)"
        ),
        end_hour: Optional[int] = Query(
            default=None,
            description="End hour (0-23)"
        ),
        email: str = Query(
            default="",
            description="User's email"
        ),
        sort_by: str = Query(
            default="total_quantity",
            description="Sort order: asc or desc"
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: asc or desc",
            regex="^(asc|desc)$"
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page"
        ),
        page: int = Query(
            default=1,
            description="Page number to return"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await purchase_tracking_db.get_purchase_report_by_email(
            email, start_date, end_date, start_hour, end_hour, sort_by, sort_order, page_size, page
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/invoice_report_by_email")
async def invoice_report_by_email(
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            default=None,
            description="Start hour (0-23)"
        ),
        end_hour: Optional[int] = Query(
            default=None,
            description="End hour (0-23)"
        ),
        email: str = Query(
            default="",
            description="User's email"
        ),
        sort_by: str = Query(
            default="total_quantity",
            description="Sort order: asc or desc"
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: asc or desc",
            regex="^(asc|desc)$"
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page"
        ),
        page: int = Query(
            default=1,
            description="Page number to return"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await purchase_tracking_db.get_invoice_report_by_email(
            email, start_date, end_date, start_hour, end_hour, sort_by, sort_order, page_size, page
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/buyer_event_chart")
async def buyer_report_chart(
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            default=None,
            description="Start hour (0-23)"
        ),
        end_hour: Optional[int] = Query(
            default=None,
            description="End hour (0-23)"
        ),
        email: str = Query(
            default="",
            description="User's email"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await purchase_tracking_db.get_purchase_report_by_email(
            email, start_date, end_date, start_hour, end_hour
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/overall_buyer_report")
async def overall_buyer_report(
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            default=None,
            description="Start hour (0-23)"
        ),
        end_hour: Optional[int] = Query(
            default=None,
            description="End hour (0-23)"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await buyer_report_db.get_overall_buyer_report(start_date, end_date, start_hour, end_hour)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/overall_event_report")
async def overall_buyer_report(
        start_date: str = Query(
            default="",
            description="Start date"
        ),
        end_date: str = Query(
            default="",
            description="End date"
        ),
        start_hour: Optional[int] = Query(
            default=None,
            description="Start hour (0-23)"
        ),
        end_hour: Optional[int] = Query(
            default=None,
            description="End hour (0-23)"
        ),
        data_type: Optional[str] = Query(
            default='event',
            description="Type of the result data"
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await event_report_db.get_overall_event_report(start_date, end_date, start_hour, end_hour)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")
