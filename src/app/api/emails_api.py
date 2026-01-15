import json
from typing import Optional, List
from datetime import datetime

from fastapi import APIRouter, Depends, Query, Body, HTTPException
from pydantic import UUID4
import pytz

from app.auth.auth_system import get_current_user_with_roles
from app.cache import handle_cache
from app.db import email_db
from app.model.create_user_email import UserEmailUpdateRequest, UserEmailCreateRequest
from app.model.email_comment_req import GroupCommentCreateRequest, EmailCommentCreateRequest, EmailForwardRequest
from app.model.email_filter import FlagUpdateRequest, TagUpdateRequest, UsersUpdateRequest, FlagBulkUpdateRequest, \
    OpenSearchFlagBulkUpdateRequest, OpenSearchFlagUpdateRequest, UsersBulkUpdateRequest, OnsaleIgnoreRequest, OnsaleMarkAddedRequest, OnsaleEmailResponse
from app.model.user import User
from app.model.daily_emails_request import DailyEmailsRequest
from app.model.individual_email_requests import TaskStatusUpdateRequest, StarStatusUpdateRequest
from app.service.email_service import EmailService
from app.utils import sqs_client, queue_url
from fastapi.responses import StreamingResponse
import io
import csv
router = APIRouter(prefix="/reports")


def convert_to_cst(dt) -> Optional[datetime]:
    """
    Convert a datetime to CST (America/Chicago) timezone.
    If the datetime is None, returns None.
    If the datetime is timezone-naive, assumes it's UTC.
    Handles both datetime objects and string representations.
    """
    if dt is None:
        return None
    
    # If it's a string, try to parse it
    if isinstance(dt, str):
        try:
            # Try parsing ISO format
            dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            # If parsing fails, return None
            return None
    
    # If it's not a datetime object, return None
    if not isinstance(dt, datetime):
        return None
    
    cst_tz = pytz.timezone("America/Chicago")
    
    # If datetime is timezone-naive, assume it's UTC
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    
    # Convert to CST
    return dt.astimezone(cst_tz)


@router.get("/v2/emails/total-messages")
async def get_total_message_count(
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    try:
        return await email_db.get_total_message_count(user.email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/v2/emails/update-total-message")
async def update_message_status(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await email_db.update_message_status(user.email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/v2/emails/list")
async def email_list_v2(
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term",
        ),
        filter_flags: Optional[str] = Query(
            default="",
            description="Filter flags",
        ),
        filter_users: Optional[str] = Query(
            default="",
            description="Filter users",
        ),
        start_date: Optional[str] = Query(
            default="",
            description="Start date for filtering",
        ),
        end_date: Optional[str] = Query(
            default="",
            description="End date for filtering",
        ),
        from_email: Optional[str] = Query(
            default="",
            description="Filter by sender email",
        ),
        to_email: Optional[str] = Query(
            default="",
            description="Filter by recipient email",
        ),
        subject: Optional[str] = Query(
            default="",
            description="Filter by subject",
        ),
        search_in: Optional[str] = Query(
            default="",
            description="Filter by status (is_starred, is_archived)",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    try:
        filter_flags_array = filter_flags.split(",") if filter_flags else []
        filter_users_array = filter_users.split(",") if filter_users else []
        key = f"email_list_v2/{page_size}/{page}/{timezone}/{search_term}/{filter_flags}/{filter_users}/{start_date}/{end_date}/{from_email}/{to_email}/{subject}/{search_in}"
        emails = await handle_cache(key, 30, email_db.get_email_list_v2, timezone, page, page_size, search_term,
                                    filter_flags_array, filter_users_array, start_date, end_date, from_email, to_email,
                                    subject, search_in)
        return emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.get("/v3/emails/list")
async def email_list_v3(
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term",
        ),
        filter_flags: Optional[str] = Query(
            default="",
            description="Filter flags",
        ),
        filter_users: Optional[str] = Query(
            default="",
            description="Filter users",
        ),
        start_date: Optional[str] = Query(
            default="",
            description="Start date for filtering",
        ),
        end_date: Optional[str] = Query(
            default="",
            description="End date for filtering",
        ),
        from_email: Optional[str] = Query(
            default="",
            description="Filter by sender email",
        ),
        to_email: Optional[str] = Query(
            default="",
            description="Filter by recipient email",
        ),
        subject: Optional[str] = Query(
            default="",
            description="Filter by subject",
        ),
        search_in: Optional[str] = Query(
            default="",
            description="Filter by status (is_starred, is_archived)",
        ),
        task_complete: Optional[bool] = Query(
            default=None,
            description="Filter by task completion status (true, false, null)",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    try:
        filter_flags_array = filter_flags.split(",") if filter_flags else []
        filter_users_array = filter_users.split(",") if filter_users else []
        key = f"email_list_v3/{page_size}/{page}/{timezone}/{search_term}/{filter_flags}/{filter_users}/{start_date}/{end_date}/{from_email}/{to_email}/{subject}/{search_in}/{task_complete}"
        emails = await handle_cache(key, 30, EmailService().get_email_list, timezone, page, page_size, search_term,
                                    filter_flags_array, filter_users_array, start_date, end_date, from_email, to_email,
                                    subject, search_in, task_complete)
        return emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/v2/emails/list/csv")
async def export_email_list_to_csv(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term",
        ),
        type: Optional[str] = Query(
            default="all",
            description="The type of export. Possible values are: "
                        "'all' for exporting all data, "
                        "'single_chain' for exporting a single chain, "
                        "'search_params' for exporting within search parameters."
        ),
        email_ids: Optional[str] = Query(
            default="",
            description="Id of the emails that is being exported when type is 'single_chain'",
        ),
        filter_flags: Optional[str] = Query(
            default="",
            description="Filter flags",
        ),
        filter_users: Optional[str] = Query(
            default="",
            description="Filter users",
        ),
        start_date: Optional[str] = Query(
            default="",
            description="Start date for filtering",
        ),
        end_date: Optional[str] = Query(
            default="",
            description="End date for filtering",
        ),
        from_email: Optional[str] = Query(
            default="",
            description="Filter by sender email",
        ),
        to_email: Optional[str] = Query(
            default="",
            description="Filter by recipient email",
        ),
        subject: Optional[str] = Query(
            default="",
            description="Filter by subject",
        ),
        search_in: Optional[str] = Query(
            default="",
            description="Filter by status (is_starred, is_archived)",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    filter_flags_array = filter_flags.split(",") if filter_flags else []
    filter_users_array = filter_users.split(",") if filter_users else []
    email_ids_array = email_ids.split(",") if email_ids else []

    message_body = {
        "timezone": timezone,
        "type": type,
        "email_ids_array": email_ids_array,
        "search_term": search_term,
        "filter_flags_array": filter_flags_array,
        "filter_users_array": filter_users_array,
        "start_date": start_date,
        "end_date": end_date,
        "from_email": from_email,
        "to_email": to_email,
        "subject": subject,
        "search_in": search_in,
        "user_email": user.email,
    }

    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body)
    )

    return {"message": "Your request is being processed. You will receive an email with the CSV link once it is ready."}


@router.post("/v2/emails/comments")
async def add_comment(
        request: GroupCommentCreateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    result = await email_db.add_comment(request.group_id, request.text, user.email)
    if result:
        return {"message": "Comment added successfully."}
    else:
        raise HTTPException(status_code=500, detail="Failed to add comment")


# OpenSearch
@router.post("/v3/emails/comments")
async def add_comment_to_email(
        request: EmailCommentCreateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
) -> dict[str, str]:
    result = await EmailService().add_comment_to_email(request.email_id, request.text, user.email)
    if result:
        return {"message": "Comment added successfully."}
    else:
        raise HTTPException(status_code=500, detail="Failed to add comment")


# OpenSearch
@router.post("/v3/email-group/comments")
async def add_comment_to_email_group(
        request: GroupCommentCreateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    result = await EmailService().add_comment_to_email_group(request.group_id, request.text, user.email)
    if result:
        return {"message": "Comment added successfully."}
    else:
        raise HTTPException(status_code=500, detail="Failed to add comment")


@router.get("/v2/emails/comments")
async def get_comments(
        group_ids: Optional[str] = Query(
            default=None,
            description="Filter flags",
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        group_ids_array = group_ids.split(",") if group_ids else []
        return await email_db.get_comments_by_group_ids(group_ids_array)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.get("/v3/emails/comments")
async def get_email_comments(
        email_id: Optional[str] = Query(
            default=None,
            description="Filter flags",
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await EmailService().get_email_comments(email_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.get("/v3/email-group/comments")
async def get_email_group_comments(
        group_id: Optional[str] = Query(
            default=None,
            description="Filter flags",
        ),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await EmailService().get_group_comments(group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/email/html/{email_id}")
async def email_by_id(email_id: str):
    key = f"email/html/{email_id}"
    return await handle_cache(key, 30, email_db.email_by_id, email_id)


# OpenSearch
@router.get("/v3/email/html/{email_id}")
async def get_email_html_v3(email_id: str):
    key = f"v3/email/html/{email_id}"
    return await handle_cache(key, 30, EmailService().get_email_content, email_id)


@router.get("/emails/list/count")
async def email_list_count(
        search_term: Optional[str] = Query(
            default=None,
            description="Search term",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    key = f"email_list_count/{search_term}"
    return handle_cache(key, 30, email_db.get_email_list_count, search_term)


@router.put("/emails/{email_id}/tags")
async def update_tags(
        email_id: str,
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        request: TagUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    await email_db.update_tags(email_id, request.tags, page_size, page)


@router.put("/emails/{email_id}/flags")
async def update_flags(
        email_id: str,
        request: FlagUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    await email_db.update_flags(email_id, request.flag_ids, request.has_archived, user.name)
    return {"message": "Flags updated successfully"}


# OpenSearch
@router.put("/v3/emails/{email_group_id}/flags")
async def update_flags_v3(
        email_group_id: str,
        request: OpenSearchFlagUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    await EmailService().validate_and_update_email_flags(email_group_id, request.flags, user.email)
    return {"message": "Flags updated successfully"}


@router.put("/emails/flags_bulk")
async def update_flags_bulk(
        request: FlagBulkUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await email_db.update_flags_bulk(request.flag_ids, request.email_ids, user.name)
        return {"message": "Flags updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.put("/v3/emails/flags-bulk")
async def update_flags_bulk_v3(
        request: OpenSearchFlagBulkUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await EmailService().validate_and_update_email_flags_bulk(
            request.email_group_ids, request.flags, user.email
        )
        return {"message": "Flags updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/emails/{email_id}/assigned_users")
async def update_assigned_users(
        email_id: str,
        request: UsersUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    await email_db.update_assigned_users(email_id, request.user_ids)
    return {"message": "users updated successfully"}


# OpenSearch
@router.put("/v3/emails/{email_group_id}/assigned-users")
async def update_assigned_users_v3(
        email_group_id: str,
        request: UsersUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    await EmailService().update_email_user_assignment(email_group_id, request.user_ids)
    return {"message": "users updated successfully"}


@router.put("/emails/users")
async def update_assigned_users_bulk(
        request: UsersBulkUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Bulk assign multiple email groups to users.
    Replaces all existing assignments for each email group with the new user_ids.
    Updates OpenSearch to maintain consistency with v3/emails/list endpoint.
    """
    try:
        await EmailService().update_email_user_assignment_bulk(request.email_group_ids, request.user_ids)
        return {"message": "Users assigned successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/emails/{email_id}/star")
async def update_email_star(
        email_id: str,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await email_db.update_email_star(email_id)
        return {"message": "email's star updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.put("/v3/emails/{email_group_id}/star")
async def update_email_star_v3(
        email_group_id: str,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await EmailService().update_email_starred(email_group_id)
        return {"message": "email's star updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.put("/v3/emails/{email_group_id}/task/status")
async def update_email_task_status_v3(
        email_group_id: str,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await EmailService().update_email_task_complete(email_group_id)
        return {"message": "email's task status updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# Individual email task status update (PostgreSQL)
@router.post("/emails/{email_id}/task/status")
async def update_individual_email_task_status(
        email_id: str,
        request: TaskStatusUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Update task completion status for an individual email by email_id.
    This endpoint updates the PostgreSQL database directly and checks if all
    emails in the duplication group are complete to update OpenSearch accordingly.
    """
    try:
        result = await email_db.update_individual_email_task_status(
            email_id, 
            request.is_task_complete
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# Individual email star status update (PostgreSQL)
@router.post("/emails/{email_id}/star/status")
async def update_individual_email_star_status(
        email_id: str,
        request: StarStatusUpdateRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Update star status for an individual email by email_id.
    This endpoint updates the PostgreSQL database directly and updates OpenSearch accordingly.
    """
    try:
        result = await email_db.update_individual_email_star_status(
            email_id, 
            request.is_starred
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.delete("/v3/emails")
async def delete_emails_v3(
        email_group_ids: List[str] = Query(..., description="List of email duplication IDs to delete"),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await EmailService().delete_emails(email_group_ids)
        return {"message": "Emails deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/email_flags")
async def get_flags(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    return await email_db.get_flags()


@router.put("/emails/{email_id}/mark_read")
async def mark_read(
        email_id: str,
        is_read: bool,
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    await email_db.mark_read(email_id, is_read)


# OpenSearch
@router.put("/v3/emails/{email_id}/mark-read")
async def mark_read_v3(
        email_id: str,
        is_read: bool,
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    await EmailService().update_email_read(email_id, is_read)


@router.put("/emails/onsale/{onsale_id}/ignore")
async def update_onsale_ignore_status(
        onsale_id: str,
        request: OnsaleIgnoreRequest,
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    """
    Update the ignored status of an onsale.
    
    Args:
        onsale_id: The ID of the onsale to update
        request: Request body containing is_ignored boolean
        user: Authenticated user
        
    Returns:
        Success message
    """
    try:
        success = await email_db.update_onsale_ignore_status(onsale_id, user.email, request.is_ignored)
        if success:
            status = "ignored" if request.is_ignored else "un-ignored"
            return {"message": f"Onsale {status} successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update onsale ignore status - check server logs for details")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.put("/emails/onsale/{onsale_id}/mark-added")
async def update_onsale_added_status(
        onsale_id: str,
        request: OnsaleMarkAddedRequest,
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    """
    Update the added status of an onsale.
    
    Args:
        onsale_id: The ID of the onsale to update
        request: Request body containing is_added boolean
        user: Authenticated user
        
    Returns:
        Success message
    """
    try:
        success = await email_db.update_onsale_added_status(onsale_id, user.email, request.is_added)
        if success:
            status = "added" if request.is_added else "removed from added"
            return {"message": f"Onsale {status} successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to update onsale added status - check server logs for details")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/emails/onsale")
async def get_onsale_email(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default=None,
            description="Search term",
        ),
        venue: Optional[List[str]] = Query(
            default=None,
            description="Filter by venue",
        ),
        start_date: Optional[str] = Query(
            default=None,
            description="Start date for filtering",
        ),
        end_date: Optional[str] = Query(
            default=None,
            description="End date for filtering",
        ),
        page_size: int = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        ),
        sort_by: str = Query(
            default="created",
            description="Column to sort by. Available options: created, venue, performer, event_name, event_datetime, onsale_or_presale_ts, discovery_date. Default sorts by onsale_or_presale_ts ASC, then event_name ASC",
        ),
        sort_order: str = Query(
            default="desc",
            description="Sort order: 'asc' or 'desc'. Default is 'asc' for onsale_or_presale_ts",
        ),
        show_empty_onsale: bool = Query(
            default=True,
            description="Whether to include records with empty/null onsale_or_presale_ts fields",
        ),
        is_added: bool = Query(
            default=False,
            description="Filter by added status",
        ),
        is_ignored: bool = Query(
            default=False,
            description="Filter by ignored status",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> OnsaleEmailResponse:
    try:
        # Validate timezone format (basic validation)
        if not timezone or '/' not in timezone:
            raise HTTPException(status_code=400, detail="Invalid timezone format. Expected format: 'Continent/City' (e.g., 'America/Chicago')")
        
        result = await email_db.get_onsale_email_details(timezone, page, page_size, search_term, venue, start_date, end_date, sort_by, sort_order, show_empty_onsale, is_added, is_ignored)
        
        # Convert datetime fields to CST timezone
        for item in result["items"]:
            if "last_received" in item:
                item["last_received"] = convert_to_cst(item["last_received"])
            if "discovery_date" in item:
                item["discovery_date"] = convert_to_cst(item["discovery_date"])
            if "added_at" in item:
                item["added_at"] = convert_to_cst(item["added_at"])
            if "ignored_at" in item:
                item["ignored_at"] = convert_to_cst(item["ignored_at"])
            if "updated_at" in item:
                item["updated_at"] = convert_to_cst(item["updated_at"])
        
        return OnsaleEmailResponse(**result)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/onsale/venues")
async def get_all_venues(
        user: User = Depends(get_current_user_with_roles(["user"])),
) -> dict:
    return await email_db.get_onsale_email_venues()


@router.get("/emails/user_emails/")
async def get_all_user_emails(
        page: int = Query(default=1, description="Page number to return"),
        page_size: int = Query(default=50, description="Number of results to return per page"),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        user_emails = await email_db.get_all_user_emails(page, page_size)
        return user_emails
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/user_emails/csv/")
async def export_user_emails_csv(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        # Get all user emails without pagination
        all_emails = await email_db.get_all_user_emails_for_export()

        # Create a string IO object to write CSV data
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header row
        writer.writerow(["ID", "Company", "Nickname", "Gmail Login", "Created At"])

        # Write data rows
        for email in all_emails:
            writer.writerow([
                email["id"],
                email["company"],
                email["nickname"],
                email["gmail_login"],
                email["created_at"]
            ])

        # Reset the position to the beginning of the buffer
        output.seek(0)

        # Return the CSV as a streaming response
        return StreamingResponse(
            content=iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=user_emails.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/users/companies")
async def get_all_user_companies(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        companies = await email_db.get_all_user_companies()
        return companies
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.post("/emails/user_emails/")
async def create_user_email(
        request: UserEmailCreateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await email_db.create_user_email(request.company, request.nickname, request.gmail_login)
        return {"message": "User email created successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/emails/user_emails/{id}")
async def update_user_email(
        id: UUID4,
        request: UserEmailUpdateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        await email_db.update_user_email(id, request)
        return {"message": "User email updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.delete("/emails/user_emails/{id}")
async def delete_user_email(
        id: UUID4,
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    try:
        await email_db.delete_user_email(id)
        return {"message": "User email deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/inactive_accounts")
async def get_inactive_accounts(
        timezone: str = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        page_size: int = Query(
            default=10,
            description="Number of results to return per page",
        ),
        page: int = Query(
            default=1,
            description="Page number to return",
        )
):
    try:
        return await email_db.get_inactive_accounts(timezone, page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/not_setup_accounts")
async def get_not_setup_accounts(
        page_size: int = Query(
            default=10,
            description="Number of results to return per page"
        ),
        page: int = Query(
            default=1,
            description="Page number to return"
        )
):
    try:
        return await email_db.get_not_setup_accounts(page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/email-group/{email_group_id}/applied-filters")
async def get_not_setup_accounts(
        email_group_id: str,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        return await EmailService().get_email_applied_filters(email_group_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.put("/emails/onsale/daily")
async def update_daily_emails(
        request: DailyEmailsRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Update the daily emails list for onsale notifications.
    This endpoint replaces the entire list of daily emails.
    
    Args:
        request: DailyEmailsRequest containing array of email strings
        user: Authenticated user
        
    Returns:
        Success message with count of emails updated
    """
    try:
        # Validate that all emails exist in the user table
        existing_emails = await email_db.validate_emails_exist(request.emails)
        
        # Check if any emails don't exist in the user table
        missing_emails = set(request.emails) - set(existing_emails)
        if missing_emails:
            raise HTTPException(
                status_code=400, 
                detail=f"The following emails do not exist in the user table: {', '.join(missing_emails)}"
            )
        
        # Replace the daily emails
        result = await email_db.replace_daily_emails(request.emails)
        
        return {
            "message": result["message"],
            "emails_count": len(request.emails),
            "validated_emails": existing_emails
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


@router.get("/emails/onsale/daily")
async def get_daily_emails(
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Get the current list of daily emails for onsale notifications.
    
    Args:
        user: Authenticated user
        
    Returns:
        List of email addresses in the daily_emails table
    """
    try:
        emails = await email_db.get_daily_emails()
        
        return {
            "emails": emails,
            "count": len(emails)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}.")


# OpenSearch
@router.post("/email/forward")
async def forward_email(
        request: EmailForwardRequest = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    """
    Forward multiple emails to specified recipients using AWS SES SendRawEmail.
    
    Args:
        request: Request body containing email_ids list and forward_to list
        user: Authenticated user
    
    Returns:
        Success message with message IDs
        
    Raises:
        HTTPException: With detailed error information if forwarding fails
    """
    try:
        # Use email_ids and forward_to from request body
        result = await EmailService().forward_emails(request.email_ids, request.forward_to)
        return {"message": "Emails forwarded successfully", "message_ids": result}
    except HTTPException:
        # Re-raise HTTPExceptions from the service layer with their original status codes and details
        raise
    except Exception as e:
        # Catch any unexpected errors and return a detailed error message
        import traceback
        error_detail = f"Unexpected error occurred while forwarding emails: {str(e)}"
        raise HTTPException(
            status_code=500,
            detail=error_detail
        )
