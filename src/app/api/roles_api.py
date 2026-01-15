import logging

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel

from app.api.users_api import DEFAULT_ROLES
from app.auth.auth_system import (
    get_current_user_with_roles,
)
from app.db import role_db
from app.model.user import User

router = APIRouter()


class NewRole(BaseModel):
    name: str


@router.get("/roles")
async def get_roles():
    return await role_db.get_all_roles()


@router.delete("/roles/{role_to_delete}")
async def delete_role(
        role_to_delete: str,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    roles = await role_db.get_all_roles()
    _raise_if_default_role(role_to_delete)
    roles = [role for role in roles if role != role_to_delete]
    await role_db.set_all_roles(roles)
    return {"message": "Role deleted successfully"}


def _raise_if_default_role(role: str):
    if role in DEFAULT_ROLES:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Cannot delete default role: {role}",
        )


@router.post("/roles")
async def create_role(
        new_role: NewRole,
        user: User = Depends(get_current_user_with_roles(["admin"])),
):
    if new_role:
        roles = await role_db.get_all_roles()
        roles.append(new_role.name.lower().strip())
        await role_db.set_all_roles(roles)
        return {"message": "Role created successfully"}


@router.post("/assign-menu-item")
async def assign_menu_item(
        role_id: str = Query(..., description="The ID of the role"),
        menu_item_id: str = Query(..., description="The ID of the menu item"),
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    await role_db.assign_menu_item(role_id, menu_item_id)


@router.post("/beta-menu-items")
async def add_menu_item_to_beta(
        menu_item_id: str = Query(..., description="The ID of the menu item to mark as beta"),
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    await role_db.add_menu_item_to_beta(menu_item_id)


@router.delete("/beta-menu-items")
async def remove_beta_menu(
        menu_item_id: str = Query(..., description="The ID of the menu item to mark as beta"),
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    await role_db.remove_beta_menu(menu_item_id)


@router.get("/menu-items")
async def get_menu_items_with_roles():
    try:
        return await role_db.get_menu_items_with_roles()
    except Exception as e:
        logging.error(f"Error fetching menu items: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.delete("/delete-menu-item")
async def remove_role_from_menu_item(
        role_id: str = Query(..., description="The ID of the role"),
        menu_item_id: str = Query(..., description="The ID of the menu item"),
        user: User = Depends(get_current_user_with_roles(["admin"]))
):
    try:
        await role_db.remove_role_from_menu_item(role_id, menu_item_id)
        return {"message": f"Role {role_id} removed from menu item {menu_item_id} successfully"}
    except Exception as e:
        raise e


@router.get("/menu-roles")
async def get_menu_roles():
    return await role_db.get_menu_roles()
