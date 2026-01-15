from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import cart_manager_filter_db
from app.model.cart_manager import AutoApprovePayload, UpdateAutoApprovePayload
from app.model.user import User

router = APIRouter(prefix="/cart-manager")


@router.post("/create_rules", response_model=dict)
async def create_cart_rules(
        payload: AutoApprovePayload,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"]))
):
    try:
        rule = await cart_manager_filter_db.create_cart_rules(payload, user.email)
        if not rule:
            raise HTTPException(status_code=400, detail="Failed to create auto-approve rule")
        return {"message": "Rule created successfully", "data": rule}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(ve)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/get_rules")
async def get_cart_rules(
        page: int = 1,
        page_size: int = 10,
        user: User = Depends(get_current_user_with_roles(["admin", "captain"]))
):
    try:
        return await cart_manager_filter_db.get_cart_rules(page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.delete("/delete_rule/{rule_id}")
async def delete_cart_rule(
        rule_id: int,
        user=Depends(get_current_user_with_roles(["admin", "captain"]))
):
    try:
        deleted_rule = await cart_manager_filter_db.soft_delete_cart_rule(rule_id)
        if not deleted_rule:
            raise HTTPException(
                status_code=404, detail=f"Rule with ID {rule_id} not found or already inactive."
            )
        return {"message": "Rule deleted successfully", "data": deleted_rule}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error: {str(e)}"
        )


@router.put("/edit_rule/{rule_id}")
async def edit_cart_rule(
        rule_id: int,
        payload: UpdateAutoApprovePayload,
        user=Depends(get_current_user_with_roles(["admin", "captain"]))
):
    try:
        updated_rule = await cart_manager_filter_db.update_cart_rule(rule_id, payload)
        if not updated_rule:
            raise HTTPException(
                status_code=404, detail=f"Rule with ID {rule_id} not found."
            )
        return {"message": "Rule updated successfully", "data": updated_rule}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Internal Server Error: {str(e)}"
        )
