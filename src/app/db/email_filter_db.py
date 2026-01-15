from app.database import get_pg_database
from app.model.email_filter import EmailFilterCreate
from app.model.user import User
from typing import List


async def save_email_filter(data: EmailFilterCreate, user: User):
    check_query = """
    SELECT 1 FROM email_filter 
    WHERE 
        archive = :archive AND 
        mark_as_read = :mark_as_read AND 
        star = :star AND 
        add_comment = :add_comment AND 
        flags = :flags AND 
        users = :users AND 
        "from" = :from_ AND 
        "to" = :to AND 
        subject = :subject AND 
        does_not_have = :does_not_have AND 
        search_term = :search_term AND
        forward_to = :forward_to
    LIMIT 1
    """

    insert_query = """
    INSERT INTO email_filter (
        archive, mark_as_read, star, add_comment,
        flags, users, "from", "to", subject, does_not_have,
        search_term, forward_to
    ) VALUES (
        :archive, :mark_as_read, :star, :add_comment,
        :flags, :users, :from_, :to, :subject, :does_not_have,
        :search_term, :forward_to
    )
    """

    insert_values = data.model_dump()

    # Check if a duplicate exists
    existing_filter = await get_pg_database().fetch_one(check_query, insert_values)
    if existing_filter:
        return {"message": "This filter already exist"}

    await get_pg_database().execute(insert_query, insert_values)
    return {"message": "Email filter created successfully"}


async def update_email_filter(filter_id: str, data: EmailFilterCreate, user: User):
    # Update existing entry
    update_query = """
    UPDATE email_filter 
    SET archive = :archive,
        mark_as_read = :mark_as_read,
        star = :star,
        add_comment = :add_comment,
        flags = :flags,
        users = :users,
        "from" = :from_,
        "to" = :to,
        subject = :subject,
        does_not_have = :does_not_have,
        search_term = :search_term,
        forward_to = :forward_to
    WHERE id = :id
    """

    update_values = data.model_dump()
    update_values["id"] = int(filter_id)

    await get_pg_database().execute(update_query, update_values)
    return {"message": "Email filter updated successfully"}


async def get_email_filter(user: User) -> List[dict]:
    # Query to fetch email filters for the logged-in user
    query = """
        SELECT id, archive, mark_as_read, star, add_comment,
               flags, users, "from", "to", subject, does_not_have,
               search_term, forward_to
        FROM email_filter
        ORDER BY id DESC
    """
    rows = await get_pg_database().fetch_all(query)

    # Convert rows to a list of dictionaries
    return [dict(row) for row in rows] if rows else []


async def delete_user_email_filter(filter_id: str):
    # Query to delete the email filter with the specified filter_id
    delete_query = """
    DELETE FROM email_filter
    WHERE id = :filter_id
    """
    delete_values = {"filter_id": int(filter_id)}

    # Execute delete query
    result = await get_pg_database().execute(delete_query, delete_values)

    # Check if deletion was successful
    if result == 0:
        raise ValueError("No email filter found with the given ID.")

    return {"message": "Email filter deleted successfully"}
