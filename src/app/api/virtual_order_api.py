import traceback

from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import virtual_order_db
from app.model.user import User
from app.model.virtual_order import VirtualOrderDto

router = APIRouter(prefix="/virtual-order")


@router.post("")
async def create_virtual_order(
        vo_input: VirtualOrderDto,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        await virtual_order_db.post_virtual_order(vo_input, user.email)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.put("/{virtual_order_id}")
async def update_virtual_order(
        virtual_order_id: str,
        vo_input: VirtualOrderDto,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        await virtual_order_db.update_virtual_order(vo_input, virtual_order_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("")
async def get_all_virtual_orders(
        page: int = 1,
        size: int = 50,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"])),
):
    try:
        return await virtual_order_db.get_all_virtual_orders(page, size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("/{vo_id}")
async def get_virtual_order_by_id(
        vo_id: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer"])),
):
    try:
        return await virtual_order_db.get_virtual_order_by_id(vo_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.delete("/{vo_id}")
async def delete_virtual_order_by_id(
        vo_id: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer"])),
):
    try:
        return await virtual_order_db.delete_virtual_order_by_id(vo_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.patch("/buyer/take_on/{vo_id}")
async def take_on_virtual_order(
        vo_id: str,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer"])),
):
    try:
        await virtual_order_db.take_on_virtual_order(vo_id, user.email)
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)
        raise HTTPException(status_code=500, detail=f"Failed: {e}")


@router.get("/buyer/virtual_orders")
async def get_buyer_virtual_orders(
        page: int = 1,
        size: int = 50,
        user: User = Depends(get_current_user_with_roles(["admin", "captain", "buyer"])),
):
    try:
        return await virtual_order_db.get_buyer_virtual_orders(user.email, size, page)
    except Exception as e:
        tb_str = traceback.format_exc()
        print(tb_str)
        raise HTTPException(status_code=500, detail=f"Failed: {e}")
