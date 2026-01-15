import asyncio

from app.service.mlx_profile_service import get_mlx_profile_service


async def create_mlx_profiles_for_accounts(
    account_id_list: list[str],
    task_name: str | None = None,
    user_email: str | None = None,
) -> None:
    """
    Entry point for FastAPI BackgroundTasks to kick off the async pipeline.
    Runs fully on the event loop without spawning orphaned tasks.
    """
    service = get_mlx_profile_service()
    await service.run(account_id_list, task_name=task_name, user_email=user_email)
