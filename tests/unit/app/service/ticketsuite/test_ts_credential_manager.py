import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from app.model.ts_config_models import TSCredentials
from app.service.ticketsuite.ts_credential_manager import (
    TSCredentialManager,
    get_automators_by_ids,
)


@pytest.mark.asyncio
async def test_get_automators_by_ids_returns_results():
    sample_rows = [
        {"id": "id-1", "name": "Shuffler", "api_key": "secret-key"},
    ]
    fetch_mock = AsyncMock(return_value=sample_rows)
    fake_db = SimpleNamespace(fetch_all=fetch_mock)

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_pg_readonly_database",
        return_value=fake_db,
    ):
        result = await get_automators_by_ids(["id-1"])

    assert result == sample_rows
    fetch_mock.assert_awaited_once()
    await_args = fetch_mock.await_args
    assert await_args is not None
    args, kwargs = await_args
    assert kwargs["values"] == {"automator_ids": ["id-1"]}


@pytest.mark.asyncio
async def test_get_automators_by_ids_handles_database_errors():
    fetch_mock = AsyncMock(side_effect=Exception("boom"))
    fake_db = SimpleNamespace(fetch_all=fetch_mock)

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_pg_readonly_database",
        return_value=fake_db,
    ):
        result = await get_automators_by_ids(["broken"])

    assert result == []
    fetch_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_load_from_database_returns_credentials():
    manager = TSCredentialManager()
    automator_id = uuid4()
    row = {
        "id": str(automator_id),
        "name": "Automator",
        "api_key": "k1",
    }
    mock_loader = AsyncMock(return_value=[row])

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_automators_by_ids",
        new=mock_loader,
    ):
        credentials = await manager._load_from_database(automator_id)

    assert credentials is not None
    assert isinstance(credentials, TSCredentials)
    assert credentials.api_key == "k1"
    mock_loader.assert_awaited_once_with([str(automator_id)])


@pytest.mark.asyncio
async def test_load_from_database_returns_none_on_missing_api_key():
    manager = TSCredentialManager()
    automator_id = uuid4()
    row = {
        "id": str(automator_id),
        "name": "Automator",
        "api_key": "",
    }
    mock_loader = AsyncMock(return_value=[row])

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_automators_by_ids",
        new=mock_loader,
    ):
        assert await manager._load_from_database(automator_id) is None

    mock_loader.assert_awaited_once_with([str(automator_id)])


@pytest.mark.asyncio
async def test_load_from_database_returns_none_when_no_result():
    manager = TSCredentialManager()
    automator_id = uuid4()
    mock_loader = AsyncMock(return_value=[])

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_automators_by_ids",
        new=mock_loader,
    ):
        assert await manager._load_from_database(automator_id) is None

    mock_loader.assert_awaited_once_with([str(automator_id)])


@pytest.mark.asyncio
async def test_load_from_database_handles_loader_exception():
    manager = TSCredentialManager()
    automator_id = uuid4()
    mock_loader = AsyncMock(side_effect=RuntimeError("boom"))

    with patch(
        "app.service.ticketsuite.ts_credential_manager.get_automators_by_ids",
        new=mock_loader,
    ):
        assert await manager._load_from_database(automator_id) is None

    mock_loader.assert_awaited_once_with([str(automator_id)])


@pytest.mark.asyncio
async def test_get_credentials_respects_cache_ttl_and_refreshes():
    manager = TSCredentialManager()
    automator_id = uuid4()
    first_creds = TSCredentials(api_key="first-key")
    second_creds = TSCredentials(api_key="second-key")
    load_mock = AsyncMock(side_effect=[first_creds, second_creds])

    with patch.object(
        TSCredentialManager, "_load_from_database", load_mock
    ), patch(
        "app.service.ticketsuite.ts_credential_manager.time.time",
        side_effect=[100.0, 105.0, 250.0, 260.0],
    ):
        first_result = await manager.get_credentials_for_automator(automator_id)
        second_result = await manager.get_credentials_for_automator(automator_id)
        third_result = await manager.get_credentials_for_automator(automator_id)

    assert first_result is first_creds
    assert second_result is first_creds
    assert third_result is second_creds
    assert load_mock.await_count == 2


def test_clear_cache_removes_stored_credentials():
    manager = TSCredentialManager()
    manager._cache["cached"] = (TSCredentials(api_key="cached-key"), 0.0)
    manager.clear_cache()
    assert manager._cache == {}
