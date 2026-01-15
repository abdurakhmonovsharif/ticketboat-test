import json
import types
from typing import Any, Dict, Optional

import pytest

from app.model.ams_models import (
    PhoneProviderType,
    TicketSuitePersonaPayload,
    TicketSuitePhone,
)
from app.service.ticketsuite.utils import (
    persona_creation_utils,
    ticketsuite_models,
    ticketsuite_utils,
)


class DummyResponse:
    def __init__(self, payload: Any, text: str = ""):
        self._payload = payload
        self.text = text

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    @property
    def content(self):
        return bytes(self.text, "utf-8")


def test_build_success_result_extracts_persona_fields():
    persona = TicketSuitePersonaPayload(
        Email="a@b.com",
        Tags="tag",
        email_id="email-id",
        primary_id="primary",
        automator_id="auto",
        account_id="acct",
    )
    result = persona_creation_utils.build_success_result(
        persona, persona_id="p-1", email="a@b.com", tags="tag"
    )
    assert result["persona_id"] == "p-1"
    assert result["primary_id"] == "primary"
    assert result["account_id"] == "acct"
    assert result["tags"] == "tag"


def test_build_failure_result_includes_optional_fields():
    persona = TicketSuitePersonaPayload(
        Email="a@b.com",
        Tags="tag",
        email_id="email-id",
        primary_id="primary",
        account_id="acct",
    )
    result = persona_creation_utils.build_failure_result(
        persona,
        email="a@b.com",
        tags="tag",
        error_msg="kaboom",
        error_type="client_error",
        status_code=400,
        response="BAD",
    )
    assert result["error"] == "kaboom"
    assert result["status_code"] == 400
    assert result["response"] == "BAD"


def test_format_phone_number_variants():
    assert persona_creation_utils.format_phone_number("12694371155") == "+1(269) 437-1155"
    assert persona_creation_utils.format_phone_number("2694371155") == "+1(269) 437-1155"
    with pytest.raises(ValueError):
        persona_creation_utils.format_phone_number("123")


def test_build_phone_data_variants(monkeypatch):
    data = {
        "PhoneNumber": "12694371155",
        "ProviderType": PhoneProviderType.PHYSICAL_PHONE,
        "Provider": "WiredSMS",
    }
    result = persona_creation_utils.build_phone_data(data)
    assert isinstance(result, TicketSuitePhone)
    assert result.Provider == "PhoneToEmail"

    data2 = {"PhoneNumber": "2694371155", "Provider": "Text Chest"}
    result2 = persona_creation_utils.build_phone_data(data2)
    assert result2.Provider == "TextChest"

    assert persona_creation_utils.build_phone_data({}) is None
    assert persona_creation_utils.build_phone_data({"PhoneNumber": "invalid"}) is None


def test_build_persona_payload_juiced_and_shadows():
    account = {
        "account_nickname": "Nick",
        "email_address": "nick@x.com",
        "proxy": {"Host": "host", "Port": 1, "Username": "user", "Password": "pass"},
        "is_shadows": True,
        "phone": {"PhoneNumber": "12694371155", "Provider": "WiredSMS", "ProviderType": PhoneProviderType.PHYSICAL_PHONE},
    }
    primary = {"primary_code": "axsmt", "password": "pw", "is_juiced": True}
    payload = persona_creation_utils.build_persona_payload(
        account=account, primary=primary, email_id="nick@x.com", primary_id="p", automator_id="auto", account_id="acct"
    )
    assert "Juiced" in payload.InternalNotes
    assert payload.SyncToAxsResale
    assert payload.InventoryTags.startswith("Nick")


@pytest.mark.asyncio
async def test_parse_json_response_success():
    response = DummyResponse({"value": 1}, text="ok")
    assert await ticketsuite_utils.parse_json_response(response) == {"value": 1}


@pytest.mark.asyncio
async def test_parse_json_response_failure():
    response = DummyResponse(ValueError("boom"), text="bad json")
    with pytest.raises(ticketsuite_models.TsError):
        await ticketsuite_utils.parse_json_response(response)


def test_retry_config_defaults():
    config = ticketsuite_models.RetryConfig()
    assert 429 in config.retryable_status_codes


def test_ts_response_accepts_extra_fields():
    data = {
        "Message": "ok",
        "Code": 200,
        "Result": {"Id": "1"},
        "Extra": "yes",
    }
    response = ticketsuite_models.TsResponse[ticketsuite_models.TsPersona](**data)  # type: ignore
    assert response.Extra == "yes"


def test_ts_error_and_client_error():
    err = ticketsuite_models.TsClientError("boom", 400, response={"text": "bad"})
    assert isinstance(err, ticketsuite_models.TsError)
    assert err.status_code == 400
    assert err.response == {"text": "bad"}


def test_proxy_payload_and_persona_models_accept_extra():
    proxy = ticketsuite_models.TsProxyPayload(Host="h", Port=1, Username="u", Password="p")
    persona = ticketsuite_models.TsPersona(Id="1", Extra="value")
    assert persona.Extra == "value"
    proxy_update = ticketsuite_models.TsPersonaProxyUpdate(Id="id", Nimble={"key": "value"})
    assert proxy_update.Nimble == {"key": "value"}
