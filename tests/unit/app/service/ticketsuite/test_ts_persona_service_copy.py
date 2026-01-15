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

    def build_success_result(**kwargs):
        return {"status": "success", **kwargs}

    def build_failure_result(**kwargs):
        return {"status": "failure", **kwargs}

    persona_utils_module.build_persona_payload = build_persona_payload
    persona_utils_module.build_success_result = build_success_result
    persona_utils_module.build_failure_result = build_failure_result
    sys.modules["app.service.ticketsuite.utils.persona_creation_utils"] = (
        persona_utils_module
    )

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

from app.service.ticketsuite.ts_persona_service_copy import (
    PersonaCreationResult,
    PersonaCreationSummary,
    TicketSuitePersonaAccountService,
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
    class DummyClient:
        def __init__(self, api_key: str, **kwargs):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {"successful": [], "failed": []}

    return TicketSuitePersonaAccountService(DummyClient)


def test_account_label_prioritizes_fields(persona_service):
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


def test_should_skip_primary_variants(persona_service):
    assert persona_service._should_skip_primary(
        FakePrimary(id="1", primary_name="P1", added_to_ts=True)
    )
    assert persona_service._should_skip_primary(
        FakePrimary(id="2", primary_name="P2", missing_fields=["foo"])
    )
    assert persona_service._should_skip_primary(
        FakePrimary(id=None, primary_name="P3")
    )
    assert not persona_service._should_skip_primary(
        FakePrimary(id="4", primary_name="P4")
    )


def test_build_account_and_primary_dict(persona_service):
    primary = FakePrimary(id="p", primary_name="P", is_juiced=True)
    account = FakeAccount(
        account_id="acct", email_id="a@b.com", email_address="a@b.com",
        account_nickname="nick", is_shadows=True, phone="123", proxy="proxy",
        primaries=[primary]
    )
    account_dict = persona_service._build_account_dict(account)
    primary_dict = persona_service._build_primary_dict(primary)
    assert account_dict["account_nickname"] == "nick"
    assert account_dict["is_shadows"]
    assert primary_dict["is_juiced"]


def test_group_and_add_personas(persona_service):
    primary = FakePrimary(id="p1", primary_name="P1")
    account = FakeAccount(
        account_id="acct", email_id="a@b.com", email_address="a@b.com",
        account_nickname="Nick", primaries=[primary]
    )
    missing_email = FakeAccount(
        account_id="acct2", email_id=None, email_address=None,
        account_nickname="NoEmail", primaries=[primary]
    )
    personas_by_auto, accounts_without = persona_service._group_personas_by_automator(
        account_primaries=[account, missing_email],
        account_automator_map={"acct": ["auto1", "auto2"]},
        primary_ids_filter={"p1"},
    )
    assert "auto1" in personas_by_auto
    bucket: Dict[str, List[Dict[str, Any]]] = {}
    persona_service._add_account_personas_to_automator(
        account=account,
        automator_id="auto3",
        personas_by_automator=bucket,
        primary_ids_filter={"p1"},
    )
    assert bucket["auto3"][0]["primary_id"] == "p1"


def test_group_personas_without_automator(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="Nick",
        primaries=[FakePrimary(id="p1", primary_name="P1")],
    )
    personas, missing = persona_service._group_personas_by_automator(
        account_primaries=[account],
        account_automator_map={},
        primary_ids_filter=None,
    )
    assert "acct" in missing
    assert not personas


def test_add_personas_respects_primary_filter(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="Nick",
        primaries=[FakePrimary(id="p1", primary_name="P1")],
    )
    bucket: Dict[str, List[Dict[str, Any]]] = {}
    persona_service._add_account_personas_to_automator(
        account=account,
        automator_id="auto",
        personas_by_automator=bucket,
        primary_ids_filter={"other"},
    )
    assert "auto" not in bucket


def test_build_payload(monkeypatch, persona_service):
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.build_persona_payload",
        lambda **kwargs: {"sentinel": kwargs},
    )
    payload = persona_service._build_ticketsuite_persona_payload({
        "account": {"id": "acct"},
        "primary": {"id": "p"},
        "email_id": "email",
        "primary_id": "p",
        "automator_id": "auto",
    }, "auto")
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
            return {
                "successful": [{"primary_id": "p", "email_id": "email"}],
                "failed": [],
            }

    persona_service._client_class = FakeClient
    update_mock = AsyncMock()
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.update_ticketsuite_persona_ids",
        update_mock,
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.build_persona_payload",
        lambda **kwargs: {"payload": "ok"},
    )
    persona_payload = {
        "account": {
            "account_nickname": "Nick",
            "email_address": "nick@x.com",
            "proxy": {"Host": "host", "Port": 1, "Username": "user", "Password": "pass"},
            "phone": {"PhoneNumber": "12694371155", "Provider": "WiredSMS"},
        },
        "primary": {"primary_code": "axsmt", "password": "pw"},
        "email_id": "email",
        "primary_id": "p",
        "automator_id": "auto",
    }
    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[persona_payload],
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

    persona_service._client_class = ExplodingClient
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.build_persona_payload",
        lambda **kwargs: {"payload": "ok"},
    )
    persona_payload = {
        "account": {
            "account_nickname": "Nick",
            "email_address": "nick@x.com",
            "proxy": {"Host": "host", "Port": 1, "Username": "user", "Password": "pass"},
            "phone": {"PhoneNumber": "12694371155", "Provider": "WiredSMS"},
        },
        "primary": {"primary_code": "axsmt", "password": "pw"},
        "email_id": "email",
        "primary_id": "p",
        "automator_id": "auto",
    }
    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[persona_payload],
    )
    assert result.failed
    assert result.error


@pytest.mark.asyncio
async def test_create_personas_in_ticketsuite_no_personas(persona_service):
    class EmptyClient:
        def __init__(self, api_key):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {"successful": [], "failed": []}

    persona_service._client_class = EmptyClient
    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[],
    )
    assert result.successful == []
    assert result.failed == []


@pytest.mark.asyncio
async def test_create_personas_in_ticketsuite_failed_batch(monkeypatch, persona_service):
    class FailureClient:
        def __init__(self, api_key):
            self.api_key = api_key

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def create_in_batch(self, personas):
            return {
                "successful": [{"primary_id": "p", "email_id": "email"}],
                "failed": [{"primary_id": "p2", "email_id": "email2", "error": "oops"}],
            }

    persona_service._client_class = FailureClient
    update_mock = AsyncMock()
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.update_ticketsuite_persona_ids",
        update_mock,
    )
    persona_payload = {
        "account": {
            "account_nickname": "Nick",
            "email_address": "nick@x.com",
            "proxy": {"Host": "host", "Port": 1, "Username": "user", "Password": "pass"},
            "phone": {"PhoneNumber": "12694371155", "Provider": "WiredSMS"},
        },
        "primary": {"primary_code": "axsmt", "password": "pw"},
        "email_id": "email",
        "primary_id": "p",
        "automator_id": "auto",
    }
    result = await persona_service._create_personas_in_ticketsuite(
        automator_id="auto",
        automator_name="Auto",
        api_key="key",
        persona_data_list=[persona_payload],
    )
    assert result.failed
    assert update_mock.await_count == 1

@pytest.mark.asyncio
async def test_create_personas_in_automator_missing_key(persona_service):
    result = await persona_service._create_personas_in_automator(
        automator={"id": "auto", "name": "Auto"},
        persona_data_list=[{"primary_id": "p", "email_id": "email"}],
    )
    assert result.failed
    assert "No API key" in result.error

@pytest.mark.asyncio
async def test_create_personas_in_automator_delegates(persona_service):
    expected = PersonaCreationResult(
        automator_id="auto",
        automator_name="Auto",
        successful=[],
        failed=[],
    )
    delegate = AsyncMock(return_value=expected)
    persona_service._create_personas_in_ticketsuite = delegate
    result = await persona_service._create_personas_in_automator(
        automator={"id": "auto", "name": "Auto", "api_key": "key"},
        persona_data_list=[{"primary_id": "p", "email_id": "email"}],
    )
    assert result is expected

@pytest.mark.asyncio
async def test_process_personas_by_automators(monkeypatch, persona_service):
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.get_automators_by_ids",
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
            "auto-ok": [{"primary_id": "p", "email_id": "email"}],
            "auto-missing": [{"primary_id": "p", "email_id": "missing"}],
        },
        accounts_without_automator=["acct"],
    )
    assert len(summary.automator_errors) >= 1
    assert any(err["automator_id"] == "auto-ok" for err in summary.automator_errors)
    assert any(err["automator_id"] == "auto-missing" for err in summary.automator_errors)
    assert summary.failed_personas


def test_transform_summary_to_sync_response(persona_service):
    summary = PersonaCreationSummary(
        total_successful=1,
        total_failed=1,
        successful_personas=[{"email_id": "success@x", "primary_id": "p1", "response": "ok", "account_id": "acct"}],
        failed_personas=[{"email_id": "fail@x", "primary_id": "p2", "error": "boom", "account_id": "acct"}],
        accounts_without_automator=[],
        automator_errors=[],
    )
    account = FakeAccount(
        account_id="acct",
        email_id="success@x",
        email_address="success@x",
        account_nickname="A",
        primaries=[FakePrimary(id="p1", primary_name="Primary")],
    )
    response = persona_service._transform_summary_to_sync_response(summary, [account])
    assert response[0]["account_id"] == "acct"
    assert any(item["status"] == "success" for item in response[0]["sync_results"])
    assert any(item["status"] == "error" for item in response[0]["sync_results"])

@pytest.mark.asyncio
async def test_sync_account_to_automator_missing_api_key(persona_service):
    result = await persona_service.sync_account_to_automator("acct", ["p"], {"id": "auto", "name": "Auto"})
    assert "No API key" in result.failed[0]["error"]

@pytest.mark.asyncio
async def test_sync_account_account_not_found(persona_service):
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[]),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert "not found" in result.error

@pytest.mark.asyncio
async def test_sync_account_missing_email(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id=None,
        email_address=None,
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="Prim")],
    )
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert "no email_id" in result.error

@pytest.mark.asyncio
async def test_sync_account_all_skipped(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="P", added_to_ts=True)],
    )
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert any("Skipped" in failure["error"] for failure in result.failed)


@pytest.mark.asyncio
async def test_sync_account_primary_not_requested(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="x@y.com",
        email_address="x@y.com",
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="P")],
    )
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", [], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert result.failed == []


@pytest.mark.asyncio
async def test_sync_account_missing_fields(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="P", missing_fields=["foo"])],
    )
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert any("missing fields" in failure["error"] for failure in result.failed)


@pytest.mark.asyncio
async def test_sync_account_success_and_exception(persona_service):
    account = FakeAccount(
        account_id="acct",
        email_id="e@x.com",
        email_address="e@x.com",
        account_nickname="A",
        primaries=[FakePrimary(id="p", primary_name="P")],
    )
    success_result = PersonaCreationResult(
        automator_id="auto",
        automator_name="Auto",
        successful=[{"primary_id": "p", "email_id": "e@x.com"}],
        failed=[],
    )
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ), patch.object(
        TicketSuitePersonaAccountService,
        "_create_personas_in_ticketsuite",
        AsyncMock(return_value=success_result),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert result.successful == success_result.successful

    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[account]),
    ), patch.object(
        TicketSuitePersonaAccountService,
        "_create_personas_in_ticketsuite",
        AsyncMock(side_effect=RuntimeError("boom")),
    ):
        result = await persona_service.sync_account_to_automator(
            "acct", ["p"], {"id": "auto", "name": "Auto", "api_key": "k"}
        )
    assert "boom" in result.error

@pytest.mark.asyncio
async def test_create_personas_for_accounts_invalid(persona_service):
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.validate_accounts_for_ts_sync",
        AsyncMock(
            return_value={
                "invalid_accounts": [
                    {"account_id": "bad", "account_nickname": "Bad", "missing_fields": ["company"]}
                ],
                "valid_accounts": [{"account_id": "good", "automator_ids": ["auto1"]}],
            }
        ),
    ):
        result = await persona_service.create_personas_for_accounts(["bad"])
    assert result[0]["account_id"] == "bad"


@pytest.mark.asyncio
async def test_create_personas_for_accounts_success(persona_service):
    account_id = "good"
    with patch(
        "app.service.ticketsuite.ts_persona_service_copy.validate_accounts_for_ts_sync",
        AsyncMock(
            return_value={
                "invalid_accounts": [],
                "valid_accounts": [{"account_id": account_id, "automator_ids": ["auto1"]}],
            }
        ),
    ), patch(
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_with_primaries",
        AsyncMock(return_value=[FakeAccount(
            account_id=account_id,
            email_id="e@x",
            email_address="e@x",
            account_nickname="Good",
            primaries=[FakePrimary(id="p", primary_name="Prim")],
        )]),
    ), patch.object(
        TicketSuitePersonaAccountService,
        "_group_personas_by_automator",
        return_value=({}, []),
    ), patch.object(
        TicketSuitePersonaAccountService,
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
        TicketSuitePersonaAccountService,
        "_transform_summary_to_sync_response",
        return_value=[{"account_id": account_id}],
    ):
        result = await persona_service.create_personas_for_accounts(
            [account_id], primary_ids=["p"], all_primaries=False
        )
    assert result == [{"account_id": account_id}]

@pytest.mark.asyncio
async def test_create_personas_for_new_accounts(monkeypatch, persona_service):
    mapping_runner = AsyncMock()
    monkeypatch.setattr(
        persona_service,
        "_create_default_primary_mappings_for_ticketboat",
        mapping_runner,
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.get_primary_ids_by_code",
        AsyncMock(return_value={INITIAL_CREATION_PRIMARIES[0]["primary_code"]: "p"}),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.validate_accounts_for_ts_sync",
        AsyncMock(return_value={"invalid_accounts": [], "valid_accounts": [{"account_id": "A", "automator_ids": []}]}),
    )
    create_for_accounts = AsyncMock(return_value={"result": {"successful_count": 1, "failed_count": 2, "automator_credential_errors": [1]}})
    monkeypatch.setattr(persona_service, "create_personas_for_accounts", create_for_accounts)
    await persona_service.create_personas_for_new_accounts(["A"])
    mapping_runner.assert_awaited_once()
    create_for_accounts.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_personas_for_new_accounts_invalid(monkeypatch, persona_service):
    mapping_runner = AsyncMock()
    monkeypatch.setattr(
        persona_service,
        "_create_default_primary_mappings_for_ticketboat",
        mapping_runner,
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.get_primary_ids_by_code",
        AsyncMock(return_value={INITIAL_CREATION_PRIMARIES[0]["primary_code"]: "p"}),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.validate_accounts_for_ts_sync",
        AsyncMock(return_value={"invalid_accounts": [{"account_id": "bad", "account_nickname": "Bad", "missing_fields": ["company"]}], "valid_accounts": []}),
    )
    create_for_accounts = AsyncMock()
    monkeypatch.setattr(persona_service, "create_personas_for_accounts", create_for_accounts)
    await persona_service.create_personas_for_new_accounts(["bad"])
    assert create_for_accounts.await_count == 0

@pytest.mark.asyncio
async def test_create_default_primary_mappings(monkeypatch, persona_service):
    monkeypatch.setattr(
        persona_service,
        "_filter_ticketboat_accounts",
        AsyncMock(return_value=["acct"]),
    )
    monkeypatch.setattr(
        "app.service.ticketsuite.ts_persona_service_copy.get_primary_ids_by_code",
        AsyncMock(return_value={"code": "primary"}),
    )
    mapping_runner = AsyncMock()
    monkeypatch.setattr(persona_service, "_create_mappings_for_accounts", mapping_runner)
    await persona_service._create_default_primary_mappings_for_ticketboat(["acct"])
    mapping_runner.assert_awaited_once()


@pytest.mark.asyncio
async def test_create_default_primary_mappings_no_accounts(monkeypatch, persona_service):
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
        "app.service.ticketsuite.ts_persona_service_copy.get_accounts_data_by_ids",
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
        "app.service.ticketsuite.ts_persona_service_copy.create_primary_account_mapping",
        mapper,
    )
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={"code": "primary"},
        primaries_config=[{"primary_code": "code", "password": "pw"}],
    )
    assert mapper.await_count == 1
    mapper.reset_mock()
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={},
        primaries_config=[{"primary_code": "missing", "password": "pw"}],
    )
    mapper.assert_not_awaited()
    mapper.side_effect = RuntimeError("boom")
    await persona_service._create_mappings_for_accounts(
        tb_accounts=["acct"],
        primary_ids={"code": "primary"},
        primaries_config=[{"primary_code": "code", "password": "pw"}],
    )


def test_format_validation_error(persona_service):
    payload = persona_service._format_validation_error([
        {"account_id": "acct", "account_nickname": "Nick", "missing_fields": ["field"]}
    ])
    assert payload["success"] is False
    assert "Missing" in payload["message"]


def test_get_persona_creation_service(monkeypatch):
    stub = object()
    monkeypatch.setattr(
        "app.model.persona_account.persona_account_factory.PersonaAccountFactory.get_service",
        lambda slug: stub,
    )
    assert get_persona_creation_service() is stub
