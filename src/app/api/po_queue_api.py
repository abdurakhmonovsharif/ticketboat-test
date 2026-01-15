import base64
import logging
import traceback
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException, Body
from pydantic import BaseModel

from app.auth.auth_system import get_current_user_with_roles
from app.db import po_queue_db
from app.model.po_queue import POUpdatePayload, POCreateRequest
from app.model.user import User
from app.service.parse_po_image_service import parse_image

router = APIRouter(prefix="/po_queue")

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@router.get("")
async def get_po(
        timezone: Optional[str] = Query(
            default="America/Chicago",
            description="Timezone for date filtering (e.g., 'America/Chicago')",
        ),
        search_term: Optional[str] = Query(
            default="",
            description="Search term to filter results"
        ),
        page_size: Optional[int] = Query(
            default=50,
            description="Number of results to return per page",
        ),
        page: Optional[int] = Query(
            default=1,
            description="Page number to return",
        ),
        user: User = Depends(get_current_user_with_roles(["user"])),
):
    return await po_queue_db.get_purchase_confirmation_data(timezone, search_term, page, page_size)


@router.put("/{po_id}")
async def update_po(
        po_id: str,
        payload: POUpdatePayload = Body(...),
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    if not po_id or not payload.status:
        raise HTTPException(status_code=400, detail="Purchase Order ID or Status is missing.")
    return await po_queue_db.update_status(po_id, payload.status.value)


class ImageData(BaseModel):
    image: str


@router.post("")
async def create_po_route(
        po_data: POCreateRequest,
        user: User = Depends(get_current_user_with_roles(["user"]))
):
    try:
        result = await po_queue_db.create_po(po_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/parse-po-image")
async def parse_po_image(data: ImageData):
    try:
        base64_data = data.image.split(',')[-1] if ',' in data.image else data.image

        # Decode the base64 string
        image_data = base64.b64decode(base64_data)

        logging.info(f"Received base64 image data of length: {len(base64_data)}")

        if len(image_data) > 6 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Image size must be less than 6 MB")

        import imghdr
        image_type = imghdr.what(None, h=image_data)
        if image_type not in ['png', 'jpeg', 'gif', 'webp']:
            raise HTTPException(status_code=400, detail="Image must be in PNG, JPEG, GIF, or WebP format")

        content_type = f"image/{image_type}"

        extracted_data = await parse_image(image_data, content_type)
        return extracted_data
    except HTTPException as he:
        raise he
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to parse the image. {str(e)}")
