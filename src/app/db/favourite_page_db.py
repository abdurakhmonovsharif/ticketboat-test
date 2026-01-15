from fastapi import HTTPException

from app.database import get_pg_database
from app.model.favourite_pages import FavouritePageCreate
from app.model.user import User


async def save_user_favourite(data: FavouritePageCreate, user: User):
    # Check for existing favorite page
    check_query = """
    SELECT 1 FROM user_favorite_pages
    WHERE user_email = :user_email
    AND page_url = :page_url
    """
    check_values = {"user_email": user.email, "page_url": data.page_url}

    existing_entry = await get_pg_database().fetch_one(check_query, check_values)

    if existing_entry:
        # Entry exists; delete the record
        delete_query = """
        DELETE FROM user_favorite_pages
        WHERE user_email = :user_email
        AND page_url = :page_url
        """
        delete_values = {
            "user_email": user.email,
            "page_url": data.page_url
        }
        await get_pg_database().execute(delete_query, delete_values)
        return {"message": "Favorite page deleted successfully"}
    else:
        # Entry does not exist; insert a new one
        insert_query = """
        INSERT INTO user_favorite_pages (user_email, page_url, page_label)
        VALUES (:user_email, :page_url, :page_label)
        """
        insert_values = {
            "user_email": user.email,
            "page_url": data.page_url,
            "page_label": data.page_label
        }
        await get_pg_database().execute(insert_query, insert_values)
        return {"message": "Favorite page created successfully"}


async def get_user_favourite(user: User):
    # Query to fetch favorite pages for the logged-in user
    query = """
        SELECT id, page_url, page_label, created_at
        FROM user_favorite_pages
        WHERE user_email = :user_email
        ORDER BY created_at DESC
    """
    values = {"user_email": user.email}

    try:
        rows = await get_pg_database().fetch_all(query, values)
        return [dict(row) for row in rows] if rows else []
    except Exception as e:
        print(f"Failed to fetch user favorite pages: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")