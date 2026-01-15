##############################################################################
# File: app_config_db.py                                                     #
# Description: Database interactions for application config values.          #
##############################################################################

from app.database import get_pg_readonly_database


async def get_config_value(config_key: str) -> str | None:
    """
    Retrieve the configuration value for the given key from the database.

    Args:
        config_key (str): The configuration key to look up.
    Returns:
        str | None: The configuration value if found, otherwise None.
    """
    entry = await get_config_entry(config_key)
    if entry:
        return entry["config_value"]
    return None


async def get_config_entry(config_key: str) -> dict[str, str] | None:
    """
    Retrieve the full configuration entry for the given key.

    Args:
        config_key (str): The configuration key to look up.
    Returns:
        dict[str, str] | None: Mapping containing config_key, config_value, and description.
    """
    query = """
    SELECT config_key, config_value, description
    FROM app_config
    WHERE config_key = :config_key
    """
    row = await get_pg_readonly_database().fetch_one(
        query, {"config_key": config_key}
    )
    if not row:
        return None
    return {
        "config_key": row["config_key"],
        "config_value": row["config_value"],
        "description": row["description"],
    }


async def get_all_config_values() -> list[dict[str, str]]:
    """
    Fetch all configuration key/value pairs and descriptions from the database.

    Returns:
        list[dict[str, str]]: Each entry contains config_key, config_value, and description.
    """
    query = """
    SELECT config_key, config_value, description
    FROM app_config
    ORDER BY config_key
    """
    rows = await get_pg_readonly_database().fetch_all(query)
    return [
        {
            "config_key": row["config_key"],
            "config_value": row["config_value"],
            "description": row["description"],
        }
        for row in rows
    ]


async def write_config_value(
    config_key: str, config_value: str, description: str | None = None
) -> None:
    """
    Write or update the configuration value for the given key in the database.

    Args:
        config_key (str): The configuration key to set.
        config_value (str): The configuration value to set.
        description (str | None): Human readable description for the config entry.
    """
    safe_description = description or config_key
    query = """
    INSERT INTO app_config (config_key, config_value, description)
    VALUES (:config_key, :config_value, :description)
    ON CONFLICT (config_key) DO UPDATE
    SET config_value = EXCLUDED.config_value,
        description = EXCLUDED.description,
        updated_at = NOW()
    """
    await get_pg_readonly_database().execute(
        query,
        {
            "config_key": config_key,
            "config_value": config_value,
            "description": safe_description,
        },
    )
