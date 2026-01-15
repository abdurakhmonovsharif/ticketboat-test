import asyncio
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientError

from app.db import ams_db
from app.model.ams_models import (
    AccountData,
    CreditCardProvider,
    OrderCreditCardResponse,
)
from app.model.credit_card_models import (
    CorpayBillingAddress,
    CorpayCardData,
    CorpayCreationData,
    CorpayCreationResponse,
    CorpayCustomerData,
    CorpayIndividualControl,
    CorpayMccGroupControl,
    CorpayMetaData,
)
from app.service.credit_card_service_base import CreditCardServiceBase


class CorpayCreditCardService(CreditCardServiceBase):
    """Corpay credit card service using Restful JSON API"""

    def __init__(self):
        super().__init__(CreditCardProvider.CORPAY)

        self.auth_url = "https://fleetcor-icd.okta.com/oauth2/aus4jf12zlOfttOcL417/v1/token"
        self.base_url = "https://api.vc.corpay.com"
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.creation_delay = 2  # seconds
        self.http_client = self._initialize_http_client()
        self.client_id = os.getenv("CORPAY_CLIENT_ID")
        self.client_secret = os.getenv("CORPAY_CLIENT_SECRET")
        self.scope = os.getenv(
            "CORPAY_SCOPE", "cards.write cards.:token.write cards.:token.read"
        )
        self._auth_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._token_lock = asyncio.Lock()

    def _initialize_http_client(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=30, sock_connect=5, sock_read=25)
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        default_headers = {
            "Accept": "application/json",
            "User-Agent": "CorpayClient/1.0",
        }

        return aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=default_headers,
        )

    def validate_account_data(self, account_data: AccountData) -> bool:
        required_fields = [
            "person_first_name",
            "person_last_name",
            "address_street_one",
            "address_city",
            "address_state",
            "address_postal_code",
            "address_country",
        ]

        missing = [
            field for field in required_fields if not getattr(account_data, field, None)
        ]

        if missing:
            self.logger.error(
                f"Account data validation failed. Missing fields: {', '.join(missing)}"
            )
            return False

        return True

    async def _get_auth_token(self) -> str:
        async with self._token_lock:
            if (
                self._auth_token
                and self._token_expiry
                and datetime.utcnow() < self._token_expiry - timedelta(seconds=60)
            ):
                return self._auth_token

            await self._refresh_auth_token()
            if not self._auth_token:
                raise RuntimeError("Unable to acquire Corpay auth token.")

            return self._auth_token

    async def _refresh_auth_token(self) -> None:
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Corpay client credentials are missing. "
                "Set CORPAY_CLIENT_ID and CORPAY_CLIENT_SECRET."
            )

        payload = {"grant_type": "client_credentials"}
        if self.scope:
            payload["scope"] = self.scope
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with self.http_client.post(
            self.auth_url,
            data=payload,
            auth=aiohttp.BasicAuth(self.client_id, self.client_secret),
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            print(data)

            if response.status != 200:
                message = data.get("error_description") or data.get("error") or "Unknown"
                raise RuntimeError(
                    f"Corpay auth failed with status {response.status}: {message}"
                )

            token = data.get("access_token")
            expires_in = int(data.get("expires_in", 3600))
            if not token:
                raise RuntimeError("Corpay auth response missing access_token.")

            self._auth_token = token
            self._token_expiry = datetime.utcnow() + timedelta(seconds=expires_in)

    def _sanitize_str(
        self, value: Optional[str], max_length: Optional[int] = None
    ) -> Optional[str]:
        if value is None:
            return None
        sanitized = value.strip()
        if max_length is not None:
            return sanitized[:max_length]
        return sanitized

    def _sanitize_phone(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None

        digits = "".join(ch for ch in value if ch.isdigit())
        if digits.startswith("1") and len(digits) > 10:
            digits = digits[1:]

        if len(digits) != 10:
            return None

        if digits[0] in {"0", "1"}:
            # Corpay enforces NANP format; omit invalid numbers rather than sending bad data
            return None

        return digits

    def _derive_employee_number(
        self,
        account_data: AccountData,
        nickname: Optional[str],
        params: Dict[str, Any],
        customer_params: Dict[str, Any],
    ) -> str:
        raw_employee_number = (
            params.get("employee_number")
            or params.get("employeeNumber")
            or params.get("corpay_employee_number")
            or customer_params.get("employeeNumber")
        )

        if raw_employee_number:
            digits_only = "".join(ch for ch in str(raw_employee_number) if ch.isdigit())
            if digits_only:
                return digits_only[:16]
            raise ValueError("Corpay employee_number must contain at least one digit.")

        nickname_source = nickname or account_data.nickname
        if nickname_source:
            trimmed = "".join(filter(str.isalnum, nickname_source.upper()))
            if trimmed:
                return trimmed[:16] or "000001"

        seed = str(account_data.id)
        namespace = str(account_data.person_id)
        unique_string = f"{seed}-{namespace}-{datetime.utcnow().timestamp()}"
        hash_value = abs(hash(unique_string))
        new_employee_number = str(hash_value)[:16].rjust(6, "0")
        return new_employee_number

    async def _apply_company_customer_data(
        self,
        params: Dict[str, Any],
    ) -> None:
        company_id = params.get("company_id")
        if not company_id:
            return

        try:
            companies = await ams_db.get_all_companies()
        except Exception as exc:
            self.logger.error(f"Corpay company lookup failed: {exc}")
            return

        company_id_str = str(company_id)
        company_name = None
        for company in companies:
            try:
                if str(company["id"]) == company_id_str:
                    company_name = company["name"] or ""
                    break
            except Exception:
                continue

        if not company_name:
            return

        company_name_normalized = company_name.lower()
        if "shadows" in company_name_normalized:
            params["corpay_customer_id"] = "DITWR"
            params["corpay_account_code"] = "A-5E8"
        elif "ticketboat" in company_name_normalized or "ticket boat" in company_name_normalized:
            params["corpay_customer_id"] = "DIUSD"
            params["corpay_account_code"] = "A-4C3"

    async def _prepare_corpay_data(
        self,
        account_data: AccountData,
        credit_limit: float,
        nickname: Optional[str],
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> CorpayCreationData:
        params = additional_params or {}

        await self._apply_company_customer_data(params)

        customer_params = params.get("corpay_customer") or {}
        customer_id = params.get("corpay_customer_id")
        account_code = params.get("corpay_account_code")

        if not customer_id or not account_code:
            raise ValueError(
                "Corpay customer_id and account_code must be supplied via additional_params."
            )

        employee_number = self._derive_employee_number(
            account_data, nickname, params, customer_params
        )

        billing_address = CorpayBillingAddress(
            addressLine1=self._sanitize_str(account_data.address_street_one, 30) or "",
            addressLine2=self._sanitize_str(account_data.address_street_two, 30),
            city=self._sanitize_str(account_data.address_city, 20) or "",
            state=self._sanitize_str(account_data.address_state, 2) or "",
            zipCode=self._sanitize_str(account_data.address_postal_code, 10) or "",
            country=self._sanitize_str(account_data.address_country or "US", 2) or "US",
        )

        set_alert_flag = params.get("set_alert_service_flag")
        if set_alert_flag is None:
            set_alert_flag = params.get("setAlertServiceFlag")
        if set_alert_flag is None:
            set_alert_flag = True

        card_kwargs: Dict[str, Any] = {
            "amount": round(float(credit_limit), 2),
            "billingAddress": billing_address,
            "emailAddress": "lindsay@ticketboat.com",
            "employeeNumber": employee_number,
            "firstName": "Lindsay",
            "lastName": "Nelson",
            "mobilePhoneNumber": self._sanitize_phone(account_data.phone_number),
            "type": "Ghost",
            "setAlertServiceFlag": bool(set_alert_flag),
        }

        individual_controls = params.get("individual_controls")
        if individual_controls:
            card_kwargs["individualControls"] = CorpayIndividualControl(
                **individual_controls
            )
        else:
            # Corpay requires billingCycle and billingCycleDay for Ghost cards.
            default_limit = min(max(float(credit_limit), 0.0), 9999.99)
            card_kwargs["individualControls"] = CorpayIndividualControl(
                billingCycle=params.get("default_billing_cycle", "weekly"),
                billingCycleDay=params.get("default_billing_cycle_day", "sunday"),
                cycleTransactionCount=params.get("default_cycle_transaction_count", 999999),
                dailyAmountLimit=default_limit,
                dailyTransactionCount=params.get("default_daily_transaction_count", 999999),
                transactionAmountLimit=default_limit,
                amount=float(credit_limit),
                mcc=None,
                open=True,
            )

        mcc_group_controls = params.get("mcc_group_controls")
        if mcc_group_controls:
            card_kwargs["mccGroupControls"] = [
                CorpayMccGroupControl(**control) for control in mcc_group_controls
            ]

        individual_mcc_controls = params.get("individual_mcc_controls")
        if individual_mcc_controls:
            card_kwargs["individualMccControls"] = [
                CorpayIndividualControl(**control)
                for control in individual_mcc_controls
            ]

        metadata = (
            params.get("metaData")
            or params.get("metadata")
            or params.get("corpay_metadata")
        )
        if metadata:
            card_kwargs["metaData"] = CorpayMetaData(**metadata)

        card_data = CorpayCardData(**card_kwargs)
        customer_data = CorpayCustomerData(
            id=str(customer_id),
            accountCode=str(account_code),
        )

        return CorpayCreationData(card=card_data, customer=customer_data)

    async def _create_card(
        self, payload: CorpayCreationData, auth_token: str
    ) -> Dict[str, Any]:
        url = f"{self.base_url}/cards"
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with self.http_client.post(
            url,
            json=payload.model_dump(mode="json", exclude_none=True),
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)

            if response.status >= 400:
                error_detail = data.get("error") if isinstance(data, dict) else data
                raise RuntimeError(
                    f"Corpay API returned {response.status}: {error_detail}"
                )

            if isinstance(data, dict):
                status = data.get("status", "SUCCEEDED")
                if status and status.upper() != "SUCCEEDED":
                    error = data.get("error") or {}
                    raise RuntimeError(
                        f"Corpay card creation failed: {error.get('message', 'Unknown error')}"
                    )
            return data

    def _format_expiration(self, expiration_date: Optional[str]) -> Optional[str]:
        if not expiration_date or len(expiration_date) != 4:
            return None

        try:
            month = int(expiration_date[:2])
            year_fragment = expiration_date[2:]
            year = int(f"20{year_fragment}") if len(year_fragment) == 2 else int(
                year_fragment
            )
            expiry = datetime(year, month, 1)
            return expiry.isoformat() + "Z"
        except ValueError:
            self.logger.warning(
                f"Unable to parse Corpay expiration date '{expiration_date}'"
            )
            return None

    def _convert_to_order_response(
        self,
        account_data: AccountData,
        corpay_response: Dict[str, Any],
    ) -> OrderCreditCardResponse:
        response_model = CorpayCreationResponse(**corpay_response)
        card = response_model.card
        card_dict = corpay_response.get("card") or {}

        expiry_iso = self._format_expiration(card_dict.get("expirationDate"))

        return self._create_success_response(
            str(account_data.id),
            card.number,
            expiry_iso,
            str(card.cvc2),
            account_token=card.token,
            provider_data=corpay_response,
        )

    async def create_credit_card(
        self,
        account_data: AccountData,
        credit_limit: float,
        nickname: Optional[str] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> OrderCreditCardResponse:
        additional_params = additional_params or {}
        self._log_request(str(account_data.id), "CREATE")

        if not self.validate_account_data(account_data):
            response = self._create_error_response(
                str(account_data.id),
                "Invalid account data for Corpay credit card creation.",
                "INVALID_ACCOUNT_DATA",
            )
            self._log_response(str(account_data.id), False, error=response.error_message)
            return response

        try:
            creation_payload = await self._prepare_corpay_data(
                account_data, credit_limit, nickname, additional_params
            )
        except ValueError as exc:
            self.logger.error(f"Corpay data preparation failed: {exc}")
            response = self._create_error_response(
                str(account_data.id), str(exc), "CORPAY_VALIDATION_ERROR"
            )
            self._log_response(str(account_data.id), False, error=response.error_message)
            return response

        try:
            token = await self._get_auth_token()
            response_data = await self._create_card(creation_payload, token)
            order_response = self._convert_to_order_response(
                account_data, response_data
            )
            self._log_response(str(account_data.id), True)
            return order_response
        except ValueError as exc:
            self.logger.error(f"Corpay card creation validation error: {exc}")
            response = self._create_error_response(
                str(account_data.id), str(exc), "CORPAY_VALIDATION_ERROR"
            )
        except ClientError as exc:
            self.logger.error(f"Network error calling Corpay API: {exc}")
            response = self._create_error_response(
                str(account_data.id),
                f"Corpay network error: {exc}",
                "CORPAY_NETWORK_ERROR",
            )
        except Exception as exc:
            self.logger.error(f"Corpay card creation failed: {exc}")
            response = self._create_error_response(
                str(account_data.id),
                f"Corpay card creation failed: {exc}",
                "CORPAY_ERROR",
            )

        self._log_response(str(account_data.id), False, error=response.error_message)
        return response
