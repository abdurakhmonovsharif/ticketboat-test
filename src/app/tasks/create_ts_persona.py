from app.service.ticketsuite.ts_persona_service import PersonaCreationService

async def create_ts_personas_for_accounts(
    account_id_list: list[str],
    task_name: str | None = None,
    user_email: str | None = None,
) -> None:
    """
    Entry point for FastAPI BackgroundTasks to kick off the async pipeline.
    Runs fully on the event loop without spawning orphaned tasks.
    """
    service = PersonaCreationService()
    await service.create_personas_for_new_accounts(account_id_list)
