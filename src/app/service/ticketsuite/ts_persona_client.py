import asyncio
import logging
from typing import Dict, Any, List, Optional, Callable
import httpx

from app.model.ams_models import TicketSuitePersonaPayload
from app.model.persona_account.persona_account_client_interface import PersonaAccountClient
from app.service.ticketsuite.utils.ticketsuite_models import (
    RetryConfig,
    TsResource,
    TsError,
    TsClientError,
    GetTsPersonaResponse,
    CreateTsPersonaResponse,
    TsPersona,
    UpdateTsPersonaResponse,
    UpdateTsPersonaProxyResponse,
    TsProxyPayload,
)
from app.service.ticketsuite.utils.ticketsuite_utils import (
    parse_json_response,
    DEFAULT_TIMEOUT,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RETRYABLE_STATUS_CODES,
)
from app.service.ticketsuite.utils.persona_creation_utils import (
    build_success_result,
    build_failure_result,
)

logger = logging.getLogger(__name__)
TICKETSUITE_API_URL = "https://theticketsuite-services.azurewebsites.net"


class TicketSuitePersonaAccountClient(PersonaAccountClient):
    """
    Client for interacting with TicketSuite Persona API.
    
    This client handles all HTTP communication with the TicketSuite Persona API,
    including connection pooling, retry logic, rate limiting, and error handling.
    
    Implements PersonaClientInterface to ensure consistency with other persona clients.
    internally for type safety and validation.
    """
    
    def __init__(
        self,
        api_key,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        retry_config: Optional[RetryConfig] = None,
        client: Optional[httpx.AsyncClient] = None
    ):
        """
        Initialize the TicketSuite Persona client.
        
        Args:
            api_key: TicketSuite API key.
            timeout: Request timeout in seconds (default: 30.0)
            retry_config: Retry configuration. If not provided, uses defaults.
            client: Optional httpx.AsyncClient for connection pooling. If not provided, creates one.
        """
        super().__init__(api_key, timeout, retry_config, client)
        self.api_key = api_key
        self.base_url = TICKETSUITE_API_URL
        self.timeout = timeout
        self.retry_config = retry_config or RetryConfig(
            max_attempts=DEFAULT_MAX_RETRIES + 1,
            retry_delay=DEFAULT_RETRY_DELAY,
            retryable_status_codes={429}
        )
        self._owns_client = client is None
        
        if self._owns_client:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)
            )
        else:
            self._client = client
        
        if not self.api_key:
            logger.warning("TicketSuite API key not configured")
    
    @property
    def client(self) -> httpx.AsyncClient:
        """Get the httpx client."""
        if self._client is None:
            raise RuntimeError("Client has been closed or was not initialized properly")
        return self._client
    
    async def close(self):
        """Close the HTTP client if we own it."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    def _get_headers(self) -> Dict[str, str]:
        """Get standard headers for TicketSuite API requests."""
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    def _get_resource_url(self, resource: TsResource) -> str:
        """Get the full URL for a TicketSuite resource."""
        return f"{self.base_url}/{resource.value}"
    
    def _should_retry(self, status_code: int) -> bool:
        """
        Determine if a request should be retried based on status code.
        
        Args:
            status_code: HTTP status code
            
        Returns:
            True if the request should be retried, False otherwise
        """
        retryable_codes = self.retry_config.retryable_status_codes
        if retryable_codes is None:
            retryable_codes = DEFAULT_RETRYABLE_STATUS_CODES
        return status_code >= 500 or status_code in retryable_codes
    

    async def _rate_limiter(
        self,
        request_func: Callable[[], Any],
        max_attempts: Optional[int] = None,
        retry_delay: Optional[float] = None
    ) -> httpx.Response:
        """
        Retry an async request function with rate limiting.
        Does not retry 4xx client errors (except 429 which is retryable).
        
        Args:
            request_func: Async function that makes the HTTP request
            max_attempts: Maximum number of total attempts including initial request (uses config default if None)
            retry_delay: Delay in seconds between retries (uses config default if None)
            
        Returns:
            Response object from successful request
            
        Raises:
            TicketSuiteClientError: For 4xx client errors (not retried)
            TicketSuiteError: If max attempts exceeded
        """
        max_attempts = max_attempts if max_attempts is not None else self.retry_config.max_attempts
        retry_delay = retry_delay if retry_delay is not None else self.retry_config.retry_delay
        
        last_exception = None
        
        for attempt in range(max_attempts):
            try:
                response = await request_func()
                logger.debug(f"TS API response: {response.status_code}")
                
                if response.status_code in (200, 201):
                    return response

                if not self._should_retry(response.status_code):
                    error_msg = f"TS API client error {response.status_code}: {response.text}"
                    logger.error(error_msg)
                    try:
                        error_response = response.json()
                    except Exception:
                        error_response = {"text": response.text}
                    
                    raise TsClientError(
                        error_msg,
                        status_code=response.status_code,
                        response=error_response
                    )
                
                logger.warning(f"TS API returned status {response.status_code}: {response.text}")
                
            except TsClientError:
                raise
            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code if e.response else 0
                if not self._should_retry(status_code):
                    raise TsClientError(
                        f"TS API client error {status_code}: {str(e)}",
                        status_code=status_code,
                        response=None
                    ) from e
                last_exception = e
                logger.error(f"TS API request failed: {e}")
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                logger.error(f"TS API request failed: {e}")
            except Exception as e:
                last_exception = e
                logger.error(f"TS API unexpected error: {e}")
            
            if attempt < max_attempts - 1:
                logger.info(f"Retrying TS API request in {retry_delay} seconds... (attempt {attempt + 2}/{max_attempts})")
                await asyncio.sleep(retry_delay)
        
        error_msg = f"Max attempts ({max_attempts}) exceeded for TS API request"
        logger.error(error_msg)
        if last_exception:
            raise TsError(error_msg) from last_exception
        raise TsError(error_msg)
    
    # ==================== Persona Operations ====================
    
    async def create(
        self,
        persona_payload: TicketSuitePersonaPayload,
    ) -> CreateTsPersonaResponse:
        """
        Create a new persona in TicketSuite.

        Args:
            persona_payload: TicketSuitePersonaPayload model with persona data

        Returns:
            CreateTsPersonaResponse (TsResponse[TsPersona]) with created persona
        """
        if not self.api_key:
            raise ValueError("TicketSuite API key is not configured")

        api_url = self._get_resource_url(TsResource.PERSONA)
        headers = self._get_headers()

        async def make_post_request():
            return await self.client.post(
                api_url,
                json=persona_payload.exclude_before_request(),
                headers=headers
            )

        response = await self._rate_limiter(make_post_request)
        raw_dict = await parse_json_response(response)
        ts_response = CreateTsPersonaResponse(**raw_dict)

        return ts_response
    
    async def get(
        self,
        persona_id: Optional[str] = None,
        email: Optional[str] = None,
        page_number: int = 0,
        page_size: int = 10
    ) -> List[TsPersona]:
        """
        Get persona(s) from TicketSuite by ID or email.

        Args:
            persona_id: Persona ID to search for
            email: Email address to search for
            page_number: Page number for pagination (default: 0)
            page_size: Results per page (default: 10)

        Returns:
            List[TsPersona] - List of matching personas (empty if none found)
        """
        if not self.api_key:
            raise ValueError("TicketSuite API key is not configured")
        
        api_url = self._get_resource_url(TsResource.PERSONA)
        headers = self._get_headers()
        
        params = {
            "PersonaAccountId": persona_id or "",
            "AccountEmail": email or "",
            "pageNumber": page_number,
            "pageSize": page_size
        }
        
        async def make_get_request():
            return await self.client.get(api_url, headers=headers, params=params)
        
        response = await self._rate_limiter(make_get_request)
        raw_dict = await parse_json_response(response)
        ts_response = GetTsPersonaResponse(**raw_dict)
        
        return ts_response.Result or []
    
    async def update(
        self,
        persona_id: str,
        persona_payload: TsPersona,
    ) -> UpdateTsPersonaResponse:
        """
        Update an existing persona in TicketSuite.
        
        Args:
            persona_id: Persona ID to update
            persona_payload: TsPersona model with updated data

        Returns:
            UpdateTsPersonaResponse (TsResponse[TsPersona]) with updated persona
        """
        if not self.api_key:
            raise ValueError("TicketSuite API key is not configured")
        
        api_url = f"{self._get_resource_url(TsResource.PERSONA)}/{persona_id}"
        headers = self._get_headers()
        api_payload = persona_payload.model_dump()

        logger.info(f"Updating persona {persona_id} with payload: {api_payload}")
        logger.info(f"API URL: {api_url}")
        
        async def make_put_request():
            return await self.client.put(api_url, json=api_payload, headers=headers)

        response = await self._rate_limiter(make_put_request)
        raw_dict = await parse_json_response(response)
        ts_response = UpdateTsPersonaResponse(**raw_dict)

        return ts_response
    
    async def delete(self, persona_id: str) -> int:
        """
        Delete a persona from TicketSuite.
        
        Args:
            persona_id: Persona ID to delete
            
        Returns:
            int - HTTP status code (200 on success)
        """
        if not self.api_key:
            raise ValueError("TicketSuite API key is not configured")
        
        api_url = f"{self._get_resource_url(TsResource.PERSONA)}/{persona_id}"
        headers = self._get_headers()
        
        async def make_delete_request():
            return await self.client.delete(api_url, headers=headers)
        
        response = await self._rate_limiter(make_delete_request)
        return response.status_code

    async def update_proxy(
        self,
        persona_id: str,
        proxy_payload: TsProxyPayload,
    ) -> UpdateTsPersonaProxyResponse:
        """
        Update persona's proxy in TicketSuite.
        
        Args:
            persona_id: Persona ID to update
            proxy_payload: TsProxyPayload (Host, Port, Username, Password)

        Returns:
            UpdateTsPersonaProxyResponse (TsResponse[TsPersonaProxyUpdate]) with updated persona
        """
        if not self.api_key:
            raise ValueError("TicketSuite API key is not configured")
        
        api_url = f"{self._get_resource_url(TsResource.PERSONA)}/UpdateProxy/{persona_id}"
        headers = self._get_headers()
        api_payload = proxy_payload.model_dump()
            
        logger.info(f"Updating persona {persona_id} proxy with payload: {api_payload}")
        logger.info(f"API URL: {api_url}")
        
        async def make_patch_request():
            return await self.client.patch(api_url, json=api_payload, headers=headers)
        
        response = await self._rate_limiter(make_patch_request)
        raw_dict = await parse_json_response(response)
        ts_response = UpdateTsPersonaProxyResponse(**raw_dict)
        return ts_response
    
    # ==================== Batch Operations ====================
    
    async def create_in_batch(
        self,
        personas: list[TicketSuitePersonaPayload],
    ) -> Dict[str, Any]:
        """
        Create multiple personas in batch.
        
        Args:
            personas: List of TicketSuitePersonaPayload models

        Returns:
            Dictionary with:
                - successful: List of successful creation results with persona_id, email_id, primary_id
                - failed: List of failed attempts with error details
                - total: Total number of personas processed
                - success_count: Number of successful creations
                - failure_count: Number of failed creations
        """
        results = {
            "successful": [],
            "failed": [],
            "total": len(personas),
            "success_count": 0,
            "failure_count": 0
        }
        
        for idx, persona in enumerate(personas):
            persona_dict = persona.model_dump()
            email = persona_dict.get('Email', 'unknown')
            tags = persona_dict.get('Tags', 'unknown')
            
            try:
                result = await self.create(persona)
                result_dict = result.model_dump()
                persona_id = (
                    result_dict.get("Result", {}).get("Id")
                    if isinstance(result_dict.get("Result"), dict)
                    else None
                )
                
                if persona_id:
                    results["successful"].append(
                        build_success_result(persona, persona_id, email, tags)
                    )
                    results["success_count"] += 1
                    logger.info(f"Persona created [{idx+1}/{len(personas)}]: {email}:{tags}")
                else:
                    error_msg = f"No persona ID in response for {email}"
                    logger.warning(error_msg)
                    results["failed"].append(
                        build_failure_result(persona, email, tags, error_msg, "missing_id")
                    )
                    results["failure_count"] += 1
                    
            except TsClientError as e:
                error_msg = f"Client error {e.status_code}: {str(e)}"
                logger.error(f"Failed to create persona [{idx+1}/{len(personas)}] for {email}:{tags} -- {error_msg}")
                results["failed"].append(
                    build_failure_result(
                        persona, email, tags, error_msg, "client_error",
                        status_code=e.status_code, response=e.response
                    )
                )
                results["failure_count"] += 1
                

            except TsError as e:
                error_msg = f"Server error after retries: {str(e)}"
                logger.error(f"Failed to create persona [{idx+1}/{len(personas)}] for {email}:{tags} -- {error_msg}")
                results["failed"].append(
                    build_failure_result(persona, email, tags, error_msg, "server_error")
                )
                results["failure_count"] += 1
                

            except Exception as e:
                error_msg = f"Unexpected error: {str(e)}"
                logger.error(f"Failed to create persona [{idx+1}/{len(personas)}] for {email}:{tags} -- {error_msg}", exc_info=True)
                results["failed"].append(
                    build_failure_result(persona, email, tags, error_msg, "unexpected_error")
                )
                results["failure_count"] += 1
                

        logger.info(
            f"Batch persona creation completed: {results['success_count']}/{results['total']} successful, "
            f"{results['failure_count']} failed"
        )
        
        return results


def get_ticketsuite_persona_client(
    api_key: str,
    timeout: float = DEFAULT_TIMEOUT,
    retry_config: Optional[RetryConfig] = None,
    client: Optional[httpx.AsyncClient] = None
) -> TicketSuitePersonaAccountClient:
    """
    Get a TicketSuite Persona client instance.
    
    Args:
        api_key: TicketSuite API key.
        timeout: Request timeout in seconds (default: 30.0)
        retry_config: Optional retry configuration
        client: Optional shared httpx.AsyncClient for connection pooling
        
    Returns:
        TicketSuitePersonaAccountClient instance
    """
    return TicketSuitePersonaAccountClient(
        api_key=api_key,
        timeout=timeout,
        retry_config=retry_config,
        client=client
    )