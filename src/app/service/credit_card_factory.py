from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from threading import Lock
from dateutil import parser

from app.db.ams_cc_db import get_all_credit_card_issuers
from app.model.ams_models import (
    AccountData,
    CreditCardProvider, 
    OrderCreditCardRequest, 
    OrderCreditCardResponse,
    CreditCardCreateRequest,
    AccountCreditCardOrders,
    BulkOrderCreditCardResponse
)
from app.service.detect_card_issuer import detect_card_issuer
from app.service.credit_card_service_base import CreditCardServiceBase
from app.service.global_rewards_credit_card_service import GlobalRewardsCreditCardService
from app.service.corpay_credit_card_service import CorpayCreditCardService
from app.service.wex_credit_card_service import WEXCreditCardService
from app.db.ams_cc_db import get_account_data_for_credit_card, log_credit_card_operation, create_credit_card
from app.utils import get_ses_client

logger = logging.getLogger(__name__)
_factory_lock = Lock()


class CreditCardFactory:
    """Factory class for managing credit card provider services"""

    def __init__(self):
        self._services: Dict[CreditCardProvider, CreditCardServiceBase] = {}
        self._initialize_services()

    def _initialize_services(self):
        """Initialize all available credit card provider services"""
        try:
            self._services[CreditCardProvider.WEX] = WEXCreditCardService()
            logger.info("WEX credit card service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize WEX service: {str(e)}")

        try:
            self._services[CreditCardProvider.GLOBAL_REWARDS] = GlobalRewardsCreditCardService()
            logger.info("Global Rewards credit card service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Global Rewards service: {str(e)}")

        try:
            self._services[CreditCardProvider.CORPAY] = CorpayCreditCardService()
            logger.info("Corpay credit card service initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Corpay service: {str(e)}")

        # TODO: Initialize other providers
        # self._services[CreditCardProvider.DIVVY] = DivvyCreditCardService()
        # self._services[CreditCardProvider.AMEX] = AMEXCreditCardService()

    def _format_card_number_for_display(
        self,
        card_number: Optional[str],
        provider: Optional[CreditCardProvider],
        empty_value: Optional[str] = "-",
        mask_non_wex: bool = False
    ) -> Optional[str]:
        """
        Format a card number for display.

        For most providers, this returns the last five digits (optionally masked).
        WEX cards remain last four digits but are masked as XXXX-XXXX-XXXX-####.
        """
        if not card_number:
            return empty_value

        if provider == CreditCardProvider.WEX:
            last_four = card_number[-4:] if len(card_number) >= 4 else card_number
            return f"XXXX-XXXX-XXXX-{last_four}"

        last_five = card_number[-5:] if len(card_number) >= 5 else card_number
        return f"*****{last_five}" if mask_non_wex else last_five

    def _sanitize_card_response(self, response: OrderCreditCardResponse) -> OrderCreditCardResponse:
        """
        Sanitize credit card response to only include masked card digits (last 5 for most,
        WEX masked with last 4) and remove CVC
        
        Args:
            response: Original response with full card details
            
        Returns:
            Sanitized response with masked card number and no CVC
        """
        if response.success:
            response.card_number = self._format_card_number_for_display(
                response.card_number,
                response.provider,
                empty_value=None,
                mask_non_wex=True
            )

        response.cvc = None

        return response

    def _send_credit_card_notification_email(
        self, card_results: List[Tuple[OrderCreditCardResponse, AccountData, Optional[str]]]
    ) -> None:
        """
        Send a consolidated notification email to AP for all credit card creations.

        Args:
            card_results: List of tuples containing (response, account_data, nickname) for each credit card
        """
        if not card_results:
            return

        try:
            ses_client = get_ses_client()
            if not ses_client:
                logger.error(
                    "Failed to get SES client for credit card notification email"
                )
                return

            # Build HTML table rows for each card
            table_rows = []
            for response, account_data, nickname in card_results:
                status = "Success" if response.success else "Failed"
                status_color = "#28a745" if response.success else "#dc3545"
                issuer = response.provider.value if response.provider else "-"

                # Get formatted card number if available (last 5 except WEX)
                card_display = "-"
                if response.success:
                    card_display = self._format_card_number_for_display(
                        response.card_number,
                        response.provider,
                        empty_value="-"
                    )

                card_nickname = nickname or "-"

                # Build billing address string
                address_parts = [account_data.address_street_one]
                if account_data.address_street_two:
                    address_parts.append(account_data.address_street_two)
                address_parts.append(
                    f"{account_data.address_city}, {account_data.address_state} {account_data.address_postal_code}"
                )
                billing_address = "<br>".join(address_parts)

                # Error message if failed
                error_info = ""
                if not response.success and response.error_message:
                    error_info = f"<br><span style='color: #dc3545; font-size: 12px;'>Error: {response.error_message}</span>"

                row = f"""
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;"><span style="color: {status_color}; font-weight: bold;">{status}</span>{error_info}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{issuer}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{card_display}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{card_nickname}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{account_data.nickname or str(account_data.id)}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{account_data.person_first_name} {account_data.person_last_name}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{account_data.email_address or '-'}</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{billing_address}</td>
                </tr>
                """
                table_rows.append(row)

            # Build the full HTML email
            card_count = len(card_results)
            success_count = sum(1 for r, _, _ in card_results if r.success)
            failed_count = card_count - success_count

            html_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th {{ background-color: #f8f9fa; padding: 12px; border: 1px solid #ddd; text-align: left; }}
                    td {{ padding: 10px; border: 1px solid #ddd; }}
                </style>
            </head>
            <body>
                <h2>Credit Card Order Summary</h2>
                <p>
                    <strong>Date:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}<br>
                    <strong>Total Cards:</strong> {card_count}<br>
                    <strong>Successful:</strong> <span style="color: #28a745;">{success_count}</span><br>
                    <strong>Failed:</strong> <span style="color: #dc3545;">{failed_count}</span>
                </p>
                <p><em>Note: Only Corpay cards were created under the name "Lindsay Nelson" with email "lindsay@ticketboat.com". The table below shows the original account holder information for reference.</em></p>
                <table>
                    <thead>
                        <tr>
                            <th>Status</th>
                            <th>Issuer</th>
                            <th>Card Last Digits</th>
                            <th>Card Nickname</th>
                            <th>Account</th>
                            <th>Cardholder Name</th>
                            <th>Email</th>
                            <th>Billing Address</th>
                        </tr>
                    </thead>
                    <tbody>
                        {"".join(table_rows)}
                    </tbody>
                </table>
            </body>
            </html>
            """

            # Create the email message
            msg = MIMEMultipart("alternative")
            msg["From"] = "forwarder@tb-portal.com"
            recipients = ["ap@ticketboat.com"]
            msg["To"] = ", ".join(recipients)
            msg["Subject"] = (
                f"Credit Card Order Summary - {datetime.now().strftime('%Y-%m-%d')}"
            )

            # Attach HTML content
            msg.attach(MIMEText(html_body, "html"))

            # Send the email
            ses_client.send_raw_email(
                Source=msg["From"],
                Destinations=recipients,
                RawMessage={"Data": msg.as_string()},
            )

            logger.info(
                f"Credit card notification email sent successfully for {card_count} card(s)"
            )

        except Exception as e:
            logger.error(f"Failed to send credit card notification email: {str(e)}")

    def get_available_providers(self) -> list[CreditCardProvider]:
        """Get list of available credit card providers"""
        return list(self._services.keys())

    def is_provider_available(self, provider: CreditCardProvider) -> bool:
        """Check if a specific provider is available"""
        return provider in self._services

    async def create_credit_card(
        self, 
        request: OrderCreditCardRequest,
        _skip_notification: bool = False
    ) -> OrderCreditCardResponse:
        """
        Create a credit card using the specified provider
        
        Args:
            request: Credit card creation request
            _skip_notification: Internal flag to skip sending notification 
                                (used by bulk_order_credit_cards to send consolidated email)
            
        Returns:
            CreditCardCreationResponse with card details or error information
        """
        logger.info(f"Processing credit card creation request for account {request.account_id} with provider {request.provider.value}")

        if not self.is_provider_available(request.provider):
            error_msg = f"Provider {request.provider.value} is not available"
            logger.error(error_msg)
            await log_credit_card_operation(
                request.account_id,
                "CREATE",
                request.provider.value,
                False,
                {"error": error_msg}
            )
            return OrderCreditCardResponse(
                success=False,
                provider=request.provider,
                account_id=request.account_id,
                error_message=error_msg,
                error_code="PROVIDER_UNAVAILABLE"
            )

        try:
            account_data = await get_account_data_for_credit_card(request.account_id)
            if not account_data:
                error_msg = f"Account {request.account_id} not found"
                logger.error(error_msg)
                await log_credit_card_operation(
                    request.account_id,
                    "CREATE",
                    request.provider.value,
                    False,
                    {"error": error_msg}
                )
                return OrderCreditCardResponse(
                    success=False,
                    provider=request.provider,
                    account_id=request.account_id,
                    error_message=error_msg,
                    error_code="ACCOUNT_NOT_FOUND"
                )

            service = self._services[request.provider]

            response: OrderCreditCardResponse = await service.create_credit_card(
                account_data,
                request.credit_limit,
                request.nickname,
                request.additional_params or {}
            )

            if response.success:
                try:
                    expiry_month = 0
                    expiry_year = 0
                    # Parse expiry date from ISO format (e.g., '2030-10-24T00:00:00.000Z')
                    if response.expiry_date:
                        try:
                            expiry_datetime = parser.parse(response.expiry_date)
                            expiry_month = expiry_datetime.month
                            expiry_year = expiry_datetime.year
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Failed to parse expiry date '{response.expiry_date}': {e}. Using defaults.")

                    if response.provider.value:
                        try:
                            issuers = await get_all_credit_card_issuers()
                            issuer_id = next(
                                (issuer["id"] for issuer in issuers if issuer["label"].lower() == response.provider.value.lower()),
                                None
                            )
                        except Exception as e:
                            logger.warning(f"Failed to get issuer ID for provider '{response.provider.value}': {e}")
                            issuer_id = None

                    card_issuer_type = detect_card_issuer(response.card_number or "")

                    card_create_request = CreditCardCreateRequest(
                        ams_account_id=str(request.account_id),
                        card_type=card_issuer_type,
                        issuer_id=str(issuer_id) if issuer_id else None,
                        card_number=response.card_number or "",
                        expiration_month=expiry_month,
                        expiration_year=expiry_year,
                        cvv=response.cvc or "",
                        ams_person_id=str(account_data.person_id),
                        avs_same_as_account=True,
                        company_id=str(account_data.company_id),
                        tm_card=False,
                        status="active",
                        type="Virtual Card",
                        secondary_card=False,
                        nickname=request.nickname,
                        created=datetime.now().isoformat() + "Z",
                        created_by="credit_card_factory"
                    )

                    card_id = await create_credit_card(card_create_request)
                    logger.info(f"Credit card stored in database with ID: {card_id}")

                    await log_credit_card_operation(
                        request.account_id,
                        "CREATE",
                        request.provider.value,
                        True,
                        {
                            "card_id": str(card_id),
                            "card_number_display": self._format_card_number_for_display(
                                response.card_number,
                                response.provider,
                                empty_value=None
                            ),
                            "credit_limit": request.credit_limit,
                            "stored_in_db": True
                        }
                    )

                except Exception as e:
                    logger.error(f"Failed to store credit card in database: {str(e)}")
                    await log_credit_card_operation(
                        request.account_id,
                        "CREATE",
                        request.provider.value,
                        True,
                        {
                            "card_created": True,
                            "storage_error": str(e),
                            "credit_limit": request.credit_limit,
                            "stored_in_db": False
                        }
                    )
            else:
                await log_credit_card_operation(
                    request.account_id,
                    "CREATE",
                    request.provider.value,
                    False,
                    {
                        "error": response.error_message,
                        "error_code": response.error_code,
                        "credit_limit": request.credit_limit
                    }
                )

            # Send notification email for single card orders
            if not _skip_notification:
                self._send_credit_card_notification_email([(response, account_data, request.nickname)])

            return response

        except Exception as e:
            error_msg = f"Unexpected error during credit card creation: {str(e)}"
            logger.error(error_msg)
            await log_credit_card_operation(
                request.account_id,
                "CREATE",
                request.provider.value,
                False,
                {"error": error_msg}
            )
            return OrderCreditCardResponse(
                success=False,
                provider=request.provider,
                account_id=request.account_id,
                error_message=error_msg,
                error_code="UNEXPECTED_ERROR"
            )

    async def bulk_order_credit_cards(
        self, 
        account_orders: List[AccountCreditCardOrders]
    ) -> BulkOrderCreditCardResponse:
        """
        Create multiple credit cards for multiple AMS accounts
        
        Args:
            account_orders: List of account orders, each containing an account_id 
                          and list of credit card orders for that account
                          
        Returns:
            BulkOrderCreditCardResponse with aggregated results and individual responses
        """
        logger.info(f"Processing bulk credit card order for {len(account_orders)} accounts")

        results = []
        total_requested = 0
        total_successful = 0
        total_failed = 0

        # Collect results for consolidated notification email
        notification_results: List[Tuple[OrderCreditCardResponse, AccountData, Optional[str]]] = []

        for account_order in account_orders:
            account_id = account_order.account_id
            card_orders = account_order.orders
            logger.info(f"Processing {len(card_orders)} card orders for account {account_id}")
            total_requested += len(card_orders)

            for card_order in card_orders:
                try:
                    individual_request = OrderCreditCardRequest(
                        account_id=account_id,
                        provider=card_order.provider,
                        credit_limit=card_order.credit_limit,
                        nickname=card_order.nickname,
                        additional_params=card_order.additional_params
                    )

                    if not self.is_provider_available(card_order.provider):
                        available_providers = [p.value for p in self.get_available_providers()]
                        error_msg = f"Provider '{card_order.provider.value}' is not available. Available providers: {available_providers}"
                        logger.warning(error_msg)

                        response = OrderCreditCardResponse(
                            success=False,
                            provider=card_order.provider,
                            account_id=account_id,
                            error_message=error_msg,
                            error_code="PROVIDER_UNAVAILABLE"
                        )
                        results.append(response)
                        total_failed += 1
                        continue

                    response = await self.create_credit_card(
                        individual_request,
                        _skip_notification=True
                    )

                    account_data = await get_account_data_for_credit_card(account_id)
                    if account_data:
                        notification_results.append((response, account_data, card_order.nickname))

                    sanitized_response = self._sanitize_card_response(response)
                    results.append(sanitized_response)

                    if response.success:
                        total_successful += 1
                        logger.info(f"Credit card created successfully for account {account_id} with provider {card_order.provider.value}")
                    else:
                        total_failed += 1
                        logger.warning(f"Credit card creation failed for account {account_id} with provider {card_order.provider.value}: {response.error_message}")

                except Exception as e:
                    error_msg = f"Error processing card order for account {account_id} with provider {card_order.provider.value}: {str(e)}"
                    logger.error(error_msg)

                    response = OrderCreditCardResponse(
                        success=False,
                        provider=card_order.provider,
                        account_id=account_id,
                        error_message=error_msg,
                        error_code="PROCESSING_ERROR"
                    )
                    results.append(response)
                    total_failed += 1

        overall_success = total_failed == 0

        logger.info(f"Bulk credit card order completed: {total_successful} successful, {total_failed} failed out of {total_requested} total")

        # Send consolidated notification email
        if notification_results:
            self._send_credit_card_notification_email(notification_results)

        return BulkOrderCreditCardResponse(
            overall_success=overall_success,
            results=results,
            total_requested=total_requested,
            total_successful=total_successful,
            total_failed=total_failed
        )


_factory_instance: Optional[CreditCardFactory] = None


def get_credit_card_factory() -> CreditCardFactory:
    """Return a singleton instance of the credit card factory."""
    global _factory_instance
    if _factory_instance is None:
        with _factory_lock:
            if _factory_instance is None:
                _factory_instance = CreditCardFactory()
    return _factory_instance
