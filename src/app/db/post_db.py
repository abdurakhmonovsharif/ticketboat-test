import os
from firebase_admin import db
from app.model.post import Post
from app.model.user import User


def create_or_replace_post(post: Post):
    ref = db.reference(f'{os.environ["ENVIRONMENT"]}/posts/{post.id}')
    ref.set(post.model_dump())


def get_post(post_id: str) -> Post | None:
    result = db.reference(f'{os.environ["ENVIRONMENT"]}/posts/{post_id}').get()
    return Post.model_validate(result) if result else None


def delete_post(post_id: str):
    db.reference(f'{os.environ["ENVIRONMENT"]}/posts/{post_id}').delete()


def get_all_posts() -> list[Post]:
    result = db.reference(f'{os.environ["ENVIRONMENT"]}/posts').get()
    if not result:
        return []
    return [Post.model_validate(p) for p in result.values()]
