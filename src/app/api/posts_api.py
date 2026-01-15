from fastapi import APIRouter, Depends, HTTPException

from app.auth.auth_system import get_current_user_with_roles
from app.db import post_db
from app.model.user import User
from app.model.post import Post

router = APIRouter()

roles = ["dev", "admin"]


@router.post("/posts")
async def create_post(
    post_data: Post,
    user: User = Depends(get_current_user_with_roles(roles)),
):
    post_db.create_or_replace_post(post_data)
    return post_data


@router.put("/posts/{post_id}")
async def update_post(
    post_id: str,
    post_data: Post,
    user: User = Depends(get_current_user_with_roles(roles)),
):
    post_db.create_or_replace_post(post_data)
    return post_data


@router.get("/posts")
async def get_posts(
    user: User = Depends(get_current_user_with_roles(roles)),
) -> list[Post]:
    return post_db.get_all_posts()


@router.get("/posts/{post_id}")
async def get_post(
    post_id: str,
    user: User = Depends(get_current_user_with_roles(roles)),
) -> Post:
    try:
        post = post_db.get_post(post_id)
        if not post:
            raise KeyError()
        return post
    except KeyError:
        raise HTTPException(status_code=404, detail="Post not found")


@router.delete("/posts/{post_id}")
async def delete_post(
    post_id: str,
    user: User = Depends(get_current_user_with_roles(roles)),
):
    post_db.delete_post(post_id)
