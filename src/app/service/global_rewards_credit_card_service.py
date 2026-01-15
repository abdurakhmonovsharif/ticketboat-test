from datetime import datetime
import os
from typing import Any, Dict, Optional

import aiohttp

from app.db import ams_db
from app.model.ams_models import (
    AccountData,
    CreditCardProvider,
    OrderCreditCardResponse,
)
from app.model.credit_card_models import (
    GlobalRewardsCreationData,
    GlobalRewardsResponse,
)
from app.service.credit_card_service_base import CreditCardServiceBase


class GlobalRewardsCreditCardService(CreditCardServiceBase):
    """Global Rewards credit card service using Restful JSON API"""

    def __init__(self):
        super().__init__(CreditCardProvider.GLOBAL_REWARDS)

        self.base_url = "https://globalrewardsusa.com/api"
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.creation_delay = 2  # seconds
        self.http_client = self._initialize_http_client()

    def _initialize_http_client(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(total=30, sock_connect=5, sock_read=25)
        connector = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        default_headers = {
            "Accept": "application/json",
            "User-Agent": "GlobalRewardsClient/1.0",
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
        ]

        missing = [
            field for field in required_fields if not getattr(account_data, field, None)
        ]

        if missing:
            self.logger.error(
                f"Validation failed for Global Rewards account data. Missing fields: {', '.join(missing)}"
            )
            return False

        return True

    async def _get_company_name(self, company_id: str) -> str | None:
        try:
            companies = await ams_db.get_all_companies()
        except Exception as exc:
            self.logger.error(f"Global Rewards company lookup failed: {exc}")
            return None

        company_id_str = str(company_id)
        for company in companies:
            try:
                if str(company["id"]) == company_id_str:
                    return company["name"] or None
            except Exception:
                continue

        return None

    def _resolve_auth_env_var(self, company_name: str) -> str | None:
        normalized = " ".join(company_name.split()).lower()
        if normalized in {"ticket boat", "ticketboat"}:
            return "GLOBAL_REWARDS_AUTH_KEY_TB_MAIN"
        if normalized == "shadows":
            return "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_MAIN"
        if normalized in {
            "ticket boat international",
            "ticket boat intl",
            "ticketboat international",
            "ticketboat intl",
        }:
            return "GLOBAL_REWARDS_AUTH_KEY_TB_INTERNATIONAL"
        if normalized in {"shadows international", "shadows intl"}:
            return "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_INTERNATIONAL"

        return None

    async def _get_auth_key(self, company_id: str) -> str | None:
        company_name = await self._get_company_name(company_id)
        if not company_name:
            return None

        env_var = self._resolve_auth_env_var(company_name)
        if not env_var:
            self.logger.warning(
                f"Global Rewards auth key mapping not found for company '{company_name}'"
            )
            return None

        return os.getenv(env_var)

    def _prepare_gr_data(
        self,
        account_data: AccountData,
        credit_limit: float,
        auth_key: str,
        nickname: str | None = None,
        bin_number: str | None = None,
    ) -> GlobalRewardsCreationData:
        card_nickname = nickname or account_data.nickname or f"Card_{account_data.id}"

        return GlobalRewardsCreationData(
            firstName=account_data.person_first_name,
            lastName=account_data.person_last_name,
            address1=account_data.address_street_one,
            address2=account_data.address_street_two or "",
            city=account_data.address_city,
            state=account_data.address_state,
            postalCode=account_data.address_postal_code,
            monthlyLimit=credit_limit,
            limitWindow="month",
            cardBin=bin_number,
            clientId=card_nickname,
            authorizationKey=auth_key,
        )

    async def _post_request(self, endpoint: str, data: dict) -> dict:
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Content-Type": "application/json",
        }
        async with self.http_client.post(url, json=data, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    def _format_expiry_date(self, exp_date: str) -> Optional[str]:
        if not exp_date:
            return None

        try:
            if len(exp_date) == 6 and exp_date.isdigit():
                year = int(exp_date[:4])
                month = int(exp_date[4:])
            elif "/" in exp_date:
                month_str, year_str = exp_date.split("/", 1)
                month = int(month_str)
                year = int(year_str)
                year += 2000 if year < 100 else 0
            else:
                return None

            formatted = datetime(year, month, 1)
            return formatted.replace(day=1).isoformat() + "Z"
        except (ValueError, TypeError):
            self.logger.warning(
                f"Unable to format Global Rewards expiry date '{exp_date}'"
            )
            return None

    def _convert_to_order_response(
        self,
        account_data: AccountData,
        gr_response: GlobalRewardsResponse,
    ) -> OrderCreditCardResponse:
        card_details = gr_response.cardDetails
        expiry_iso = self._format_expiry_date(card_details.expDate)

        return OrderCreditCardResponse(
            success=True,
            provider=self.provider,
            account_id=account_data.id,
            card_number=card_details.cardNumber,
            expiry_date=expiry_iso,
            cvc=card_details.cvc,
            account_token=gr_response.globalrewardsId,
            provider_data=gr_response.model_dump(mode="json"),
        )

    async def create_credit_card(
        self,
        account_data: AccountData,
        credit_limit: float,
        nickname: Optional[str] = None,
        additional_params: Optional[Dict[str, Any]] = None,
    ) -> OrderCreditCardResponse:
        self.logger.info("Creating Global Rewards credit card...")

        if not self.validate_account_data(account_data):
            return self._create_error_response(
                str(account_data.id),
                "Invalid account data for Global Rewards credit card creation.",
                "INVALID_ACCOUNT_DATA",
            )

        try:
            company_id = additional_params.get("company_id") if additional_params else None
            if company_id:
                auth_key = await self._get_auth_key(company_id)
                if not auth_key:
                    raise ValueError(f"No auth key found for company '{company_id}'")

                bin_number = additional_params.get("bin_number") if additional_params else None
                gr_data = self._prepare_gr_data(
                    account_data, credit_limit, auth_key, nickname, bin_number
                )
                response = await self._post_request(
                    endpoint="/s3/instantcard/create",
                    data=gr_data.model_dump(),
                )
                gr_response = GlobalRewardsResponse(**response)
                self.logger.info("Global Rewards credit card created successfully.")
                return self._convert_to_order_response(account_data, gr_response)
            else:
                raise ValueError("Company ID is required in additional_params.")
        except Exception as e:
            self.logger.error(f"Error creating Global Rewards credit card: {e}")
            return self._create_error_response(
                str(account_data.id),
                f"Global Rewards card creation failed: {e}",
                "GLOBAL_REWARDS_ERROR",
            )
