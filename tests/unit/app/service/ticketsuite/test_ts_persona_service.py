import json
import sys
import types
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pytest
from unittest.mock import AsyncMock, patch

if "app.service.geocode_ams_address" not in sys.modules:
    geocode_module = types.ModuleType("app.service.geocode_ams_address")

    async def geocode_ams_address(address: str) -> Optional[str]:
        return None

    geocode_module.geocode_ams_address = geocode_ams_address
    sys.modules["app.service.geocode_ams_address"] = geocode_module

if "app.service.ticketsuite.utils.persona_creation_utils" not in sys.modules:
    persona_utils_module = types.ModuleType(
        "app.service.ticketsuite.utils.persona_creation_utils"
    )

    def build_persona_payload(**kwargs):
        return kwargs

    persona_utils_module.build_persona_payload = build_persona_payload
    sys.modules["app.service.ticketsuite.utils.persona_creation_utils"] = (
        persona_utils_module
    )

if "app.service.ticketsuite.ts_persona_client" not in sys.modules:
    ts_client_module = types.ModuleType("app.service.ticketsuite.ts_persona_client")

    class DummyTicketsuiteClient:
        def __init__(self, api_key: str):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {"successful": [], "failed": []}

    ts_client_module.get_ticketsuite_persona_client = lambda api_key: DummyTicketsuiteClient(
        api_key
    )
    ts_client_module.TicketSuitePersonaAccountClient = DummyTicketsuiteClient
    sys.modules["app.service.ticketsuite.ts_persona_client"] = ts_client_module

if "app.db.ams_db" not in sys.modules:
    ams_db_module = types.ModuleType("app.db.ams_db")
    ams_db_module.get_accounts_with_primaries = AsyncMock(return_value=[])
    ams_db_module.get_automators_by_ids = AsyncMock(return_value=[])
    ams_db_module.validate_accounts_for_ts_sync = AsyncMock(
        return_value={"invalid_accounts": [], "valid_accounts": []}
    )
    ams_db_module.get_primary_ids_by_code = AsyncMock(return_value={})
    ams_db_module.create_primary_account_mapping = AsyncMock()
    ams_db_module.get_accounts_data_by_ids = AsyncMock(return_value=[])
    ams_db_module.update_ticketsuite_persona_ids = AsyncMock()
    sys.modules["app.db.ams_db"] = ams_db_module

from app.service.ticketsuite.ts_persona_service import (
    PersonaCreationResult,
    PersonaCreationService,
    PersonaCreationSummary,
    get_persona_creation_service,
    INITIAL_CREATION_PRIMARIES,
)


@dataclass
class FakePrimary:
    id: Optional[str]
    primary_name: str
    added_to_ts: bool = False
    missing_fields: Optional[List[str]] = None
    is_juiced: bool = False

    def model_dump(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "primary_name": self.primary_name,
            "added_to_ts": self.added_to_ts,
            "missing_fields": self.missing_fields,
        }


@dataclass
class FakeAccount:
    account_id: str
    email_id: Optional[str]
    email_address: Optional[str]
    account_nickname: str
    is_shadows: bool = False
    phone: str = ""
    proxy: str = ""
    primaries: List[FakePrimary] = ()

    def model_dump(self) -> Dict[str, Any]:
        return {
            "account_id": self.account_id,
            "email_id": self.email_id,
            "email_address": self.email_address,
            "account_nickname": self.account_nickname,
            "is_shadows": self.is_shadows,
            "phone": self.phone,
            "proxy": self.proxy,
            "primaries": [p.model_dump() for p in self.primaries],
        }


@pytest.fixture
def persona_service():
    return PersonaCreationService()


def test_account_label(persona_service):
    assert (
        persona_service._account_label(
            {"account_nickname": "nick", "email_address": "email"}
        )
        == "nick"
    )
    assert (
        persona_service._account_label({"nickname": "alias", "account_id": "1"})
        == "alias"
    )
    assert (
        persona_service._account_label({"email_address": "a@b.com", "account_id": "1"})
        == "a@b.com"
    )
    assert persona_service._account_label({}) == "unknown account"


def test_company_helpers(persona_service):
    assert persona_service._is_ticketboat_company("Ticket Boat")
    assert not persona_service._is_ticketboat_company("None")
    assert persona_service._is_shadows_company("Shadows")
    assert not persona_service._is_shadows_company("None")


def test_should_skip_primary(persona_service):
    assert persona_service._should_skip_primary(
        FakePrimary(id="1", primary_name="Prim", added_to_ts=True)
    )
    assert persona_service._should_skip_primary(
        FakePrimary(id="2", primary_name="Prim", missing_fields=["foo"])
    )
    assert persona_service._should_skip_primary(
        FakePrimary(id=None, primary_name="Prim")
    )
    assert not persona_service._should_skip_primary(
        FakePrimary(id="3", primary_name="Prim")
    )


def test_group_and_add(persona_service):
    primary = FakePrimary(id="p1", primary_name="P1")
    account = FakeAccount(
        account_id="acct", email_id="a@b.com", email_address="a@b.com",
        account_nickname="Nick", primaries=[primary]
    )
    missing_email = FakeAccount(
        account_id="acct-no", email_id=None, email_address=None,
        account_nickname="NoEmail", primaries=[primary]
    )
    personas_by_auto, accounts_without = persona_service._group_personas_by_automator(
        account_primaries=[account, missing_email],
        account_automator_map={"acct": ["auto1"]},
        primary_ids_filter={"p1"},
    )
    assert "auto1" in personas_by_auto
    assert "acct-no" not in accounts_without
    bucket: Dict[str, List[Dict[str, Any]]] = {}
    persona_service._add_account_personas_to_automator(
        account=account,
        automator_id="auto-z",
        personas_by_automator=bucket,
        primary_ids_filter={"p1"},
    )
    assert bucket["auto-z"][0]["primary_id"] == "p1"


def test_group_accounts_without_automator(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="Nick",
        primaries=[FakePrimary(id="p1", primary_name="Prim")],
    )
    personas, missing = persona_service._group_personas_by_automator(
        account_primaries=[account],
        account_automator_map={},
        primary_ids_filter=None,
    )
    assert personas == {}
    assert missing == ["acct"]


def test_add_account_personas_filters(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="Nick",
        primaries=[
            FakePrimary(id="p1", primary_name="Prim"),
            FakePrimary(id="skip", primary_name="Skip", missing_fields=["foo"]),
        ],
    )
    bucket: Dict[str, List[Dict[str, Any]]] = {}
    persona_service._add_account_personas_to_automator(
        account=account,
        automator_id="auto",
        personas_by_automator=bucket,
        primary_ids_filter={"p1"},
    )
    assert bucket["auto"][0]["primary_id"] == "p1"


def test_build_payload(monkeypatch, persona_service):
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.build_persona_payload",
        lambda **kwargs: {"sentinel": kwargs},
    )
    payload = persona_service._build_ticketsuite_persona_payload(
        {
            "account": {"id": "acct"},
            "primary": {"id": "p"},
            "email_id": "email",
            "primary_id": "p",
            "automator_id": "auto",
        },
        "auto",
    )
    assert payload["sentinel"]["account"]["id"] == "acct"

@pytest.mark.asyncio
async def test_create_personas_in_ticketsuite_success(monkeypatch, persona_service):
    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {"successful": [{"primary_id": "p", "email_id": "e"}], "failed": []}

    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_ticketsuite_persona_client",
        lambda api_key: FakeClient(api_key),
    )
    update_mock = AsyncMock()
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.update_ticketsuite_persona_ids",
        update_mock,
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.build_persona_payload",
        lambda **kwargs: {"payload": "ok"},
    )

    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[{"account": {}, "primary": {}, "email_id": "e", "primary_id": "p", "automator_id": "auto"}],
    )
    assert result.successful
    update_mock.assert_awaited_once()

@pytest.mark.asyncio
async def test_create_personas_in_ticketsuite_failure(monkeypatch, persona_service):
    class ExplodingClient:
        def __init__(self, api_key):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_ticketsuite_persona_client",
        lambda api_key: ExplodingClient(api_key),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.build_persona_payload",
        lambda **kwargs: {"payload": "ok"},
    )

    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[{"account": {}, "primary": {}, "email_id": "e", "primary_id": "p", "automator_id": "auto"}],
    )

    assert result.failed
    assert "Error creating personas" in result.failed[0]["error"]


@pytest.mark.asyncio
async def test_create_personas_in_ticketsuite_empty(persona_service):
    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[],
    )
    assert result.successful == []
    assert result.failed == []


@pytest.mark.asyncio
async def test_create_personas_in_automator_missing_key(persona_service):
    result = await persona_service._create_personas_in_automator(
        automator={"id": "auto", "name": "Auto"},
        persona_data_list=[{"email_id": "e", "primary_id": "p"}],
    )
    assert result.failed
    assert "No API key" in result.error


@pytest.mark.asyncio
async def test_create_personas_in_automator_success(monkeypatch, persona_service):
    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {"successful": [], "failed": []}

    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_ticketsuite_persona_client",
        lambda api_key: FakeClient(api_key),
    )
    result = await persona_service._create_personas_in_automator(
        automator={"id": "auto", "name": "Auto", "api_key": "key"},
        persona_data_list=[{"email_id": "e", "primary_id": "p"}],
    )
    assert result.successful == []

@pytest.mark.asyncio
async def test_process_personas_by_automators(monkeypatch, persona_service):
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_automators_by_ids",
        AsyncMock(return_value=[{"id": "auto-ok", "name": "Auto", "api_key": "key"}]),
    )

    async def fake_create(automator, persona_data_list):
        return PersonaCreationResult(
            automator_id=automator["id"],
            automator_name=automator["name"],
            successful=[{"email_id": "email", "primary_id": "p"}],
            failed=[],
            error="oops",
        )

    persona_service._create_personas_in_automator = AsyncMock(side_effect=fake_create)

    summary = await persona_service._process_personas_by_automators(
        {
            "auto-ok": [{"email_id": "email", "primary_id": "p"}],
            "auto-missing": [{"email_id": "missing", "primary_id": "p"}],
        },
        accounts_without_automator=["acct"],
    )

    assert summary.automator_errors
    assert summary.successful_personas


def test_transform_summary_to_sync_response(persona_service):
    summary = PersonaCreationSummary(
        total_successful=1,
        total_failed=1,
        successful_personas=[{"email_id": "success@x", "primary_id": "p", "response": "ok", "account_id": "acct"}],
        failed_personas=[{"email_id": "fail@x", "primary_id": "p2", "error": "boom", "account_id": "acct"}],
        accounts_without_automator=[],
        automator_errors=[],
    )
    account = FakeAccount(
        account_id="acct",
        email_id="success@x",
        email_address="success@x",
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="Primary")],
    )
    response = persona_service._transform_summary_to_sync_response(summary, [account])
    assert response[0]["account_id"] == "acct"
    assert any(item["status"] == "success" for item in response[0]["sync_results"])


def test_transform_summary_lookup_by_email(persona_service):
    summary = PersonaCreationSummary(
        total_successful=1,
        total_failed=0,
        successful_personas=[{"email_id": "unknown@x", "primary_id": "p"}],
        failed_personas=[],
        accounts_without_automator=[],
        automator_errors=[],
    )
    account = FakeAccount(
        account_id="acct",
        email_id="unknown@x",
        email_address="unknown@x",
        account_nickname="X",
        primaries=[FakePrimary(id="p", primary_name="Primary")],
    )
    response = persona_service._transform_summary_to_sync_response(summary, [account])
    assert response[0]["account_id"] == "acct"

@pytest.mark.asyncio
async def test_create_personas_for_accounts_invalid(persona_service):
    with patch(
        "app.service.ticketsuite.ts_persona_service.validate_accounts_for_ts_sync",
        AsyncMock(
            return_value={
                "invalid_accounts": [
                    {"account_id": "bad", "account_nickname": "Bad", "missing_fields": ["company"]}
                ],
                "valid_accounts": [],
            }
        ),
    ):
        result = await persona_service.create_personas_for_accounts(["bad"])
    assert result[0]["account_id"] == "bad"

@pytest.mark.asyncio
async def test_create_personas_for_accounts_success(persona_service):
    account_id = "good"
    with patch(
        "app.service.ticketsuite.ts_persona_service.validate_accounts_for_ts_sync",
        AsyncMock(
            return_value={
                "invalid_accounts": [],
                "valid_accounts": [{"account_id": account_id, "automator_ids": ["auto1"]}],
            }
        ),
    ), patch(
        "app.service.ticketsuite.ts_persona_service.get_accounts_with_primaries",
        AsyncMock(return_value=[FakeAccount(
            account_id=account_id,
            email_id="e@x",
            email_address="e@x",
            account_nickname="Good",
            primaries=[FakePrimary(id="p", primary_name="Prim")],
        )]),
    ), patch.object(
        PersonaCreationService,
        "_group_personas_by_automator",
        return_value=({}, []),
    ), patch.object(
        PersonaCreationService,
        "_process_personas_by_automators",
        AsyncMock(return_value=PersonaCreationSummary(
            total_successful=0,
            total_failed=0,
            successful_personas=[],
            failed_personas=[],
            accounts_without_automator=[],
            automator_errors=[],
        )),
    ), patch.object(
        PersonaCreationService,
        "_transform_summary_to_sync_response",
        return_value=[{"account_id": account_id}],
    ):
        result = await persona_service.create_personas_for_accounts(
            [account_id], primary_ids=["p"], all_primaries=False
        )
    assert result == [{"account_id": account_id}]

@pytest.mark.asyncio
async def test_create_personas_for_new_accounts(monkeypatch, persona_service):
    monkeypatch.setattr(
        persona_service,
        "_create_default_primary_mappings_for_ticketboat",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_primary_ids_by_code",
        AsyncMock(return_value={INITIAL_CREATION_PRIMARIES[0]["primary_code"]: "primary"}),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.validate_accounts_for_ts_sync",
        AsyncMock(return_value={"invalid_accounts": [], "valid_accounts": [{"account_id": "A", "automator_ids": []}]}),
    )
    create_for_accounts = AsyncMock(return_value={"result": {"successful_count": 1, "failed_count": 1, "automator_credential_errors": [1]}})
    monkeypatch.setattr(persona_service, "create_personas_for_accounts", create_for_accounts)
    await persona_service.create_personas_for_new_accounts(["A"])
    create_for_accounts.assert_awaited_once()

@pytest.mark.asyncio
async def test_create_default_primary_mappings(monkeypatch, persona_service):
    monkeypatch.setattr(
        persona_service,
        "_filter_ticketboat_accounts",
        AsyncMock(return_value=["acct"]),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_primary_ids_by_code",
        AsyncMock(return_value={"code": "primary"}),
    )
    mapping_runner = AsyncMock()
    monkeypatch.setattr(persona_service, "_create_mappings_for_accounts", mapping_runner)
    await persona_service._create_default_primary_mappings_for_ticketboat(["acct"])
    mapping_runner.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_default_primary_mappings_no_ticketboat(monkeypatch, persona_service):
    monkeypatch.setattr(
        persona_service,
        "_filter_ticketboat_accounts",
        AsyncMock(return_value=[]),
    )
    mapping_runner = AsyncMock()
    monkeypatch.setattr(persona_service, "_create_mappings_for_accounts", mapping_runner)
    await persona_service._create_default_primary_mappings_for_ticketboat(["acct"])
    mapping_runner.assert_not_awaited()

@pytest.mark.asyncio
async def test_filter_ticketboat_accounts(monkeypatch, persona_service):
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.get_accounts_data_by_ids",
        AsyncMock(
            return_value=[
                {"id": "1", "company": json.dumps({"name": "Ticket Boat"})},
                {"id": "2", "company": json.dumps({"name": "Shadows"})},
                {"id": "3", "company": None},
            ]
        ),
    )
    result = await persona_service._filter_ticketboat_accounts(["1", "2", "3"])
    assert result == ["1"]

@pytest.mark.asyncio
async def test_create_mappings_for_accounts(monkeypatch, persona_service):
    async def fake_mapping(account_id, primary_id, password):
        return "done"

    mapper = AsyncMock(side_effect=fake_mapping)
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.create_primary_account_mapping",
        mapper,
    )
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={"code": "primary"},
        primaries_config=[{"primary_code": "code", "password": "pw"}],
    )
    assert mapper.await_count == 1


@pytest.mark.asyncio
async def test_create_mappings_for_accounts_missing_code(monkeypatch, persona_service):
    mapper = AsyncMock()
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.create_primary_account_mapping",
        mapper,
    )
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={},
        primaries_config=[{"primary_code": "missing", "password": "pw"}],
    )
    mapper.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_mappings_for_accounts_handles_errors(monkeypatch, persona_service):
    mapper = AsyncMock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service.create_primary_account_mapping",
        mapper,
    )
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={"code": "primary"},
        primaries_config=[{"primary_code": "code", "password": "pw"}],
    )
    assert mapper.await_count >= 1

def test_format_validation_error(persona_service):
    payload = persona_service._format_validation_error([
        {"account_id": "acct", "account_nickname": "Nick", "missing_fields": ["field"]}
    ])
    assert payload["success"] is False
    assert "Missing" in payload["message"]


def test_get_persona_creation_service():
    first = get_persona_creation_service()
    second = get_persona_creation_service()
    assert isinstance(first, PersonaCreationService)
    assert first is not second
