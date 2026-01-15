import asyncio
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from app.model.ams_models import TicketSuitePersonaPayload
from app.service.ticketsuite.ts_persona_client import (
    TicketSuitePersonaAccountClient,
    get_ticketsuite_persona_client,
)
from app.service.ticketsuite.utils.ticketsuite_models import (
    CreateTsPersonaResponse,
    TsClientError,
    TsError,
    TsPersona,
    TsProxyPayload,
    TsResource,
    TsResponse,
    UpdateTsPersonaProxyResponse,
    UpdateTsPersonaResponse,
)


def _make_client_stub():
    return cast(
        httpx.AsyncClient,
        SimpleNamespace(
            post=AsyncMock(),
            get=AsyncMock(),
            put=AsyncMock(),
            delete=AsyncMock(),
            patch=AsyncMock(),
        ),
    )


def _sample_persona_payload(**kwargs):
    return TicketSuitePersonaPayload(
        Email="test@example.com",
        Tags="tag",
        **kwargs,
    )


class FakeResponse:
    def __init__(self, status_code: int, text: str = "", json_data=None, json_exc=None):
        self.status_code = status_code
        self.text = text
        self._json_data = {} if json_data is None else json_data
        self._json_exc = json_exc

    def json(self):
        if self._json_exc:
            raise self._json_exc
        return self._json_data


async def _passthrough_rate_limiter(request_fn):
    return await request_fn()


def test_headers_and_resource_url():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())

    headers = client._get_headers()
    assert headers["Authorization"] == "Bearer api-key"
    assert headers["Content-Type"] == "application/json"

    resource_url = client._get_resource_url(TsResource.PERSONA)
    assert resource_url.endswith("personaAccount")


def test_should_retry_behavior():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    assert client._should_retry(500)
    assert client._should_retry(429)
    assert not client._should_retry(404)


@pytest.mark.asyncio
async def test_rate_limiter_succeeds_without_retries():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    response = httpx.Response(200, json={"Message": "ok"})

    async def request_func():
        return response

    assert await client._rate_limiter(request_func) is response


@pytest.mark.asyncio
async def test_rate_limiter_raises_client_error_for_4xx():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    response = httpx.Response(400, json={"detail": "bad request"})

    async def request_func():
        return response

    with pytest.raises(TsClientError) as exc:
        await client._rate_limiter(request_func)

    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_rate_limiter_retries_and_raises_ts_error_after_max_attempts():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    response = httpx.Response(500, text="boom")
    request_func = AsyncMock(return_value=response)

    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        AsyncMock(),
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts


@pytest.mark.asyncio
async def test_create_uses_rate_limiter_and_parses_response():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    create_response = {
        "Message": "Created",
        "Result": {"Id": "persona-1"},
    }

    async def rate_limiter(request_fn):
        return httpx.Response(201, json={})

    with patch.object(client, "_rate_limiter", new=AsyncMock(side_effect=rate_limiter)):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value=create_response),
        ):
            payload = _sample_persona_payload()
            result = await client.create(payload)

    assert isinstance(result, TsResponse)
    assert result.Result is not None
    assert result.Result.Id == "persona-1"


@pytest.mark.asyncio
async def test_get_returns_personas_list():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    get_response = {
        "Message": "Ok",
        "Result": [{"Id": "p1"}],
    }

    async def rate_limiter(request_fn):
        return httpx.Response(200, json={})

    with patch.object(client, "_rate_limiter", new=AsyncMock(side_effect=rate_limiter)):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value=get_response),
        ):
            personas = await client.get(persona_id="foo")

    assert len(personas) == 1
    assert isinstance(personas[0], TsPersona)
    assert personas[0].Id == "p1"


@pytest.mark.asyncio
async def test_create_in_batch_successful_result():
    persona = _sample_persona_payload()
    ts_persona = TsPersona(Id="generated")
    response = CreateTsPersonaResponse(Result=ts_persona)

    with patch.object(
        TicketSuitePersonaAccountClient,
        "create",
        new=AsyncMock(return_value=response),
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
        results = await client.create_in_batch([persona])

    assert results["success_count"] == 1
    assert results["failure_count"] == 0
    assert results["successful"][0]["persona_id"] == "generated"


@pytest.mark.asyncio
async def test_create_in_batch_handles_client_error():
    persona = _sample_persona_payload()
    error = TsClientError("bad request", status_code=401, response={"detail": "missing"})

    with patch.object(
        TicketSuitePersonaAccountClient,
        "create",
        new=AsyncMock(side_effect=error),
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
        results = await client.create_in_batch([persona])

    assert results["success_count"] == 0
    assert results["failure_count"] == 1
    assert results["failed"][0]["error_type"] == "client_error"


@pytest.mark.asyncio
async def test_create_in_batch_handles_server_error():
    persona = _sample_persona_payload()
    error = TsError("server boom")

    with patch.object(
        TicketSuitePersonaAccountClient,
        "create",
        new=AsyncMock(side_effect=error),
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
        results = await client.create_in_batch([persona])

    assert results["failure_count"] == 1
    assert results["failed"][0]["error_type"] == "server_error"


@pytest.mark.asyncio
async def test_create_in_batch_handles_unexpected_error():
    persona = _sample_persona_payload()

    with patch.object(
        TicketSuitePersonaAccountClient,
        "create",
        new=AsyncMock(side_effect=ValueError("explode")),
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
        results = await client.create_in_batch([persona])

    assert results["failure_count"] == 1
    assert results["failed"][0]["error_type"] == "unexpected_error"


@pytest.mark.asyncio
async def test_update_returns_response_model():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    update_payload = TsPersona(Id="p1", Email="test")
    update_response = {"Result": {"Id": "p1"}}

    async def rate_limiter(fn):
        return httpx.Response(200, json={})

    with patch.object(client, "_rate_limiter", new=AsyncMock(side_effect=rate_limiter)):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value=update_response),
        ):
            result = await client.update("p1", update_payload)

    assert isinstance(result, TsResponse)
    assert result.Result is not None
    assert result.Result.Id == "p1"


@pytest.mark.asyncio
async def test_delete_returns_status_code():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())

    async def rate_limiter(fn):
        return httpx.Response(200, json={})

    with patch.object(client, "_rate_limiter", new=AsyncMock(side_effect=rate_limiter)):
        status = await client.delete("p1")

    assert status == 200


@pytest.mark.asyncio
async def test_update_proxy_returns_proxy_response():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    proxy_payload = TsProxyPayload(
        Host="host",
        Port=8080,
        Username="user",
        Password="pass",
    )
    proxy_response = {"Result": {"Id": "p1"}}

    async def rate_limiter(fn):
        return httpx.Response(200, json={})

    with patch.object(client, "_rate_limiter", new=AsyncMock(side_effect=rate_limiter)):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value=proxy_response),
        ):
            result = await client.update_proxy("p1", proxy_payload)

    assert isinstance(result, TsResponse)
    assert result.Result is not None
    assert result.Result.Id == "p1"


@pytest.mark.asyncio
async def test_create_invokes_http_client():
    stub_client = _make_client_stub()
    stub_client.post.return_value = httpx.Response(201, json={})
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub_client)

    with patch.object(
        client,
        "_rate_limiter",
        new=AsyncMock(side_effect=_passthrough_rate_limiter),
    ):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value={"Result": {"Id": "persona"}}),
        ):
            await client.create(_sample_persona_payload())

    stub_client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_invokes_http_client():
    stub_client = _make_client_stub()
    stub_client.get.return_value = httpx.Response(200, json={})
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub_client)

    with patch.object(
        client,
        "_rate_limiter",
        new=AsyncMock(side_effect=_passthrough_rate_limiter),
    ):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value={"Result": [{"Id": "p1"}]}),
        ):
            await client.get(persona_id="foo")

    stub_client.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_invokes_http_client():
    stub_client = _make_client_stub()
    stub_client.put.return_value = httpx.Response(200, json={})
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub_client)

    async def dummy_rate(request_fn):
        return await request_fn()

    with patch.object(
        client,
        "_rate_limiter",
        new=AsyncMock(side_effect=dummy_rate),
    ):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value={"Result": {"Id": "p1"}}),
        ):
            await client.update("id", TsPersona())

    stub_client.put.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_invokes_http_client():
    stub_client = _make_client_stub()
    stub_client.delete.return_value = httpx.Response(200, json={})
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub_client)

    with patch.object(
        client,
        "_rate_limiter",
        new=AsyncMock(side_effect=_passthrough_rate_limiter),
    ):
        await client.delete("id")

    stub_client.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_proxy_invokes_http_client():
    stub_client = _make_client_stub()
    stub_client.patch.return_value = httpx.Response(200, json={})
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub_client)

    proxy_payload = TsProxyPayload(Host="h", Port=1, Username="u", Password="p")

    with patch.object(
        client,
        "_rate_limiter",
        new=AsyncMock(side_effect=_passthrough_rate_limiter),
    ):
        with patch(
            "app.service.ticketsuite.ts_persona_client.parse_json_response",
            new=AsyncMock(return_value={"Result": {"Id": "proxy"}}),
        ):
            await client.update_proxy("id", proxy_payload)

    stub_client.patch.assert_awaited_once()


def test_init_owns_client_configuration():
    fake_client = SimpleNamespace(aclose=AsyncMock())

    def fake_async_client(*args, **kwargs):
        return fake_client

    with patch(
        "app.service.ticketsuite.ts_persona_client.httpx.AsyncClient",
        side_effect=fake_async_client,
    ) as async_client_ctor:
        client = TicketSuitePersonaAccountClient(api_key="api-key")

    assert client._owns_client
    async_client_ctor.assert_called_once()


def test_client_property_raises_when_client_missing():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    client._client = None
    with pytest.raises(RuntimeError):
        _ = client.client


def test_client_property_returns_http_client():
    stub = _make_client_stub()
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=stub)
    assert client.client is stub


@pytest.mark.asyncio
async def test_close_owned_client_closes_httpx_client():
    fake_client = SimpleNamespace(aclose=AsyncMock())

    with patch(
        "app.service.ticketsuite.ts_persona_client.httpx.AsyncClient",
        return_value=fake_client,
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key")

    await client.close()
    fake_client.aclose.assert_awaited_once()
    assert client._client is None


@pytest.mark.asyncio
async def test_context_manager_calls_close():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())

    with patch.object(
        TicketSuitePersonaAccountClient,
        "close",
        new=AsyncMock(),
    ) as close_mock:
        async with client as ctx:
            assert ctx is client

    close_mock.assert_awaited_once()


def test_should_retry_handles_none_retry_codes():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    client.retry_config.retryable_status_codes = None
    assert client._should_retry(429)


@pytest.mark.asyncio
async def test_rate_limiter_retries_on_429_until_ts_error():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    response = FakeResponse(429, text="rate limit")
    request_func = AsyncMock(return_value=response)

    sleep_mock = AsyncMock()
    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        sleep_mock,
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts
    assert sleep_mock.await_count == client.retry_config.max_attempts - 1


@pytest.mark.asyncio
async def test_rate_limiter_client_error_handles_bad_json():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    response = FakeResponse(400, text="bad", json_exc=ValueError("boom"))

    request_func = AsyncMock(return_value=response)

    with pytest.raises(TsClientError) as exc:
        await client._rate_limiter(request_func)

    assert exc.value.status_code == 400
    assert exc.value.response == {"text": "bad"}


@pytest.mark.asyncio
async def test_rate_limiter_http_status_error_non_retry_raises_client():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    resp = httpx.Response(400, request=httpx.Request("GET", "https://example.com"))
    error = httpx.HTTPStatusError("error", request=resp.request, response=resp)
    request_func = AsyncMock(side_effect=error)

    with pytest.raises(TsClientError) as exc:
        await client._rate_limiter(request_func)

    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_rate_limiter_http_status_error_retry_raises_ts_error():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    resp = httpx.Response(503, request=httpx.Request("GET", "https://example.com"))
    error = httpx.HTTPStatusError("error", request=resp.request, response=resp)
    request_func = AsyncMock(side_effect=error)
    sleep_mock = AsyncMock()

    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        sleep_mock,
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts
    assert sleep_mock.await_count == client.retry_config.max_attempts - 1


@pytest.mark.asyncio
async def test_rate_limiter_handles_request_error_with_retries():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    error = httpx.RequestError("fail", request=httpx.Request("GET", "https://example.com"))
    request_func = AsyncMock(side_effect=error)
    sleep_mock = AsyncMock()

    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        sleep_mock,
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts


@pytest.mark.asyncio
async def test_rate_limiter_handles_timeout_exception_with_retries():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    error = httpx.TimeoutException("timeout")
    request_func = AsyncMock(side_effect=error)
    sleep_mock = AsyncMock()

    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        sleep_mock,
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts


@pytest.mark.asyncio
async def test_rate_limiter_handles_unexpected_exception():
    client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
    request_func = AsyncMock(side_effect=ValueError("boom"))
    sleep_mock = AsyncMock()

    with patch(
        "app.service.ticketsuite.ts_persona_client.asyncio.sleep",
        sleep_mock,
    ):
        with pytest.raises(TsError):
            await client._rate_limiter(request_func)

    assert request_func.await_count == client.retry_config.max_attempts


@pytest.mark.asyncio
async def test_create_requires_api_key():
    client = TicketSuitePersonaAccountClient(api_key="", client=_make_client_stub())
    with pytest.raises(ValueError):
        await client.create(_sample_persona_payload())


@pytest.mark.asyncio
async def test_get_requires_api_key():
    client = TicketSuitePersonaAccountClient(api_key="", client=_make_client_stub())
    with pytest.raises(ValueError):
        await client.get()


@pytest.mark.asyncio
async def test_update_requires_api_key():
    client = TicketSuitePersonaAccountClient(api_key="", client=_make_client_stub())
    with pytest.raises(ValueError):
        await client.update("id", TsPersona())


@pytest.mark.asyncio
async def test_delete_requires_api_key():
    client = TicketSuitePersonaAccountClient(api_key="", client=_make_client_stub())
    with pytest.raises(ValueError):
        await client.delete("id")


@pytest.mark.asyncio
async def test_update_proxy_requires_api_key():
    client = TicketSuitePersonaAccountClient(api_key="", client=_make_client_stub())
    proxy_payload = TsProxyPayload(Host="x", Port=1, Username="u", Password="p")
    with pytest.raises(ValueError):
        await client.update_proxy("id", proxy_payload)


@pytest.mark.asyncio
async def test_create_in_batch_handles_missing_persona_id():
    persona = _sample_persona_payload()
    response = CreateTsPersonaResponse(Result=None)

    with patch.object(
        TicketSuitePersonaAccountClient,
        "create",
        new=AsyncMock(return_value=response),
    ):
        client = TicketSuitePersonaAccountClient(api_key="api-key", client=_make_client_stub())
        results = await client.create_in_batch([persona])

    assert results["failure_count"] == 1
    assert results["failed"][0]["error_type"] == "missing_id"


def test_get_ticketsuite_persona_client_factory():
    stub = _make_client_stub()
    with patch(
        "app.service.ticketsuite.ts_persona_client.TicketSuitePersonaAccountClient",
        return_value=stub,
    ) as factory:
        result = get_ticketsuite_persona_client(api_key="api-key")

    assert result is stub
    factory.assert_called_once()
