import os
import asyncio
import aiohttp
from typing import Dict, Any, Optional
from xml.etree import ElementTree as ET

from app.model.ams_models import (
    AccountData, 
    OrderCreditCardResponse, 
    CreditCardProvider
)
from app.model.credit_card_models import (
    WEXAccountCreationData,
    WEXAccountResponse
)
from app.model.wex_config_models import WEXCredentials
from app.service.credit_card_service_base import CreditCardServiceBase
from app.service.wex_credential_manager import wex_credential_manager


class WEXCreditCardService(CreditCardServiceBase):
    """WEX credit card creation service using SOAP API"""
    
    def __init__(self):
        super().__init__(CreditCardProvider.WEX)
        
        self.base_url = "https://services.encompass-suite.com/services/AccountService.asmx"
        self.create_headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "http://aocsolutions.com/EncompassWebServices/CreateAccountRealTime"
        }
        self.retrieve_headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": "http://aocsolutions.com/EncompassWebServices/GetAccountDataInternationalExtended"
        }
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        self.creation_delay = 2  # seconds
        
    
    def validate_account_data(self, account_data: AccountData) -> bool:
        """Validate required fields for WEX account creation"""
        required_fields = [
            'person_first_name', 'person_last_name', 'address_street_one',
            'address_city', 'address_state', 'address_postal_code', 'address_country'
        ]
        
        for field in required_fields:
            if not getattr(account_data, field, None):
                self.logger.error(f"Missing required field for WEX: {field}")
                return False
        
        return True
    
    def _prepare_wex_data(self, account_data: AccountData, credit_limit: float, nickname: Optional[str] = None) -> WEXAccountCreationData:
        """Convert AMS account data to WEX format"""
        account_nickname = account_data.nickname or ""
        name_line2 = nickname.strip() if nickname else account_nickname
        
        return WEXAccountCreationData(
            last_name=account_data.person_last_name,
            first_name=account_data.person_first_name,
            name_line2=name_line2,
            address_line1=account_data.address_street_one,
            city=account_data.address_city,
            state=account_data.address_state,
            zip=account_data.address_postal_code,
            country=account_data.address_country,
            credit_limit=credit_limit
        )
    
    def _create_account_soap_request(self, wex_data: WEXAccountCreationData, credentials: WEXCredentials) -> str:
        """Create SOAP request XML for account creation"""
        return f"""<?xml version="1.0" encoding="utf-8"?>
                    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                        <soap:Body>
                            <CreateAccountRealTime xmlns="http://aocsolutions.com/EncompassWebServices/">
                                <user>
                                    <OrgGroupLoginId>{credentials.org_group_login_id}</OrgGroupLoginId>
                                    <Username>{credentials.username}</Username>
                                    <Password>{credentials.password}</Password>
                                </user>
                                <request>
                                    <AccountType>Static</AccountType>
                                    <BankNumber>{credentials.bank_number}</BankNumber>
                                    <CompanyNumber>{credentials.company_number}</CompanyNumber>
                                    <LastName>{wex_data.last_name}</LastName>
                                    <FirstName>{wex_data.first_name}</FirstName>
                                    <NameLine2>{wex_data.name_line2}</NameLine2>
                                    <AddressLine1>{wex_data.address_line1}</AddressLine1>
                                    <City>{wex_data.city}</City>
                                    <State>{wex_data.state}</State>
                                    <Zip>{wex_data.zip}</Zip>
                                    <Country>{wex_data.country}</Country>
                                    <CreditLimit>{wex_data.credit_limit}</CreditLimit>
                                </request>
                            </CreateAccountRealTime>
                        </soap:Body>
                    </soap:Envelope>"""
    
    def _create_retrieval_soap_request(self, account_token: str, credentials: WEXCredentials) -> str:
        """Create SOAP request XML for account data retrieval"""
        return f"""<?xml version="1.0" encoding="utf-8"?>
                    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:enc="http://aocsolutions.com/EncompassWebServices/">
                        <soapenv:Header/>
                        <soapenv:Body>
                            <enc:GetAccountDataInternationalExtended>
                                <enc:user>
                                    <enc:OrgGroupLoginId>{credentials.org_group_login_id}</enc:OrgGroupLoginId>
                                    <enc:Username>{credentials.username}</enc:Username>
                                    <enc:Password>{credentials.password}</enc:Password>
                                </enc:user>
                                <enc:request>
                                    <enc:AccountToken>{account_token}</enc:AccountToken>
                                </enc:request>
                            </enc:GetAccountDataInternationalExtended>
                        </soapenv:Body>
                    </soapenv:Envelope>"""
    
    def _safe_find_text(self, element, path: str, namespace: Dict[str, str], default: str = '') -> str:
        """Safely extract text from XML element with fallback"""
        found = element.find(path, namespace)
        return found.text if found is not None else default
    
    def _process_creation_response(self, xml_content: str) -> tuple[str, str]:
        """Process WEX account creation response. Returns (token, description)"""
        try:
            root = ET.fromstring(xml_content)
            namespace = {'ns': 'http://aocsolutions.com/EncompassWebServices/'}
            
            token_element = root.find('.//ns:AccountToken', namespace)
            desc_element = root.find('.//ns:Description', namespace)
            response_code = root.find('.//ns:ResponseCode', namespace)
            
            token = token_element.text if token_element is not None and token_element.text else ""
            description = desc_element.text if desc_element is not None and desc_element.text else "Unknown error"
            
            if response_code is not None and response_code.text != "Success":
                return "", f"ERROR - {description}"
            elif not token:
                return "", "ERROR - No account token received"
            else:
                return token, description
                
        except ET.ParseError as e:
            return "", f"ERROR - Failed to parse XML response: {str(e)}"
        except Exception as e:
            return "", f"ERROR - Unexpected error processing response: {str(e)}"
    
    def _process_retrieval_response(self, xml_content: str) -> WEXAccountResponse:
        """Process WEX account data retrieval response"""
        try:
            root = ET.fromstring(xml_content)
            namespace = {'ns': 'http://aocsolutions.com/EncompassWebServices/'}
            
            result = root.find('.//ns:GetAccountDataInternationalExtendedResult', namespace)
            if result is None:
                return WEXAccountResponse(
                    success=False,
                    description="No result data found in retrieval response"
                )
            
            description = self._safe_find_text(result, 'ns:Description', namespace)
            if "not found" in description.lower():
                return WEXAccountResponse(
                    success=False,
                    description=description
                )
            
            return WEXAccountResponse(
                success=True,
                description=description,
                card_number=self._safe_find_text(result, 'ns:AccountNumber', namespace),
                expiry_date=self._safe_find_text(result, 'ns:ExpireDate', namespace),
                cvc=self._safe_find_text(result, 'ns:Cvc', namespace),
                credit_rating=self._safe_find_text(result, 'ns:CreditRating', namespace),
                status_code=self._safe_find_text(result, './/ns:StatusCode', namespace)
            )
            
        except ET.ParseError as e:
            return WEXAccountResponse(
                success=False,
                description=f"Failed to parse XML response: {str(e)}"
            )
        except Exception as e:
            return WEXAccountResponse(
                success=False,
                description=f"Unexpected error processing response: {str(e)}"
            )
    
    async def _make_soap_request(self, soap_body: str, headers: Dict[str, str]) -> str:
        """Make async SOAP request to WEX API"""
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.base_url,
                data=soap_body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                if response.status != 200:
                    raise Exception(f"HTTP {response.status}: {await response.text()}")
                return await response.text()
    
    async def create_credit_card(
        self, 
        account_data: AccountData, 
        credit_limit: float,
        nickname: Optional[str] = None,
        additional_params: Optional[Dict[str, Any]] = None
    ) -> OrderCreditCardResponse:
        """Create WEX credit card for the given account"""
        
        self._log_request(str(account_data.id), "create", credit_limit=credit_limit)
        
        if not self.validate_account_data(account_data):
            return self._create_error_response(
                str(account_data.id),
                "Invalid account data for WEX card creation"
            )
        
        credentials = wex_credential_manager.get_credentials_for_company(account_data.company_id)
        self.logger.info(f"WEX credentials: {credentials}")
        if not credentials:
            return self._create_error_response(
                str(account_data.id),
                "No WEX credentials available for this account's company",
                "NO_CREDENTIALS"
            )
        
        try:
            wex_data = self._prepare_wex_data(account_data, credit_limit, nickname)
            print(f"WEX data: {wex_data}")
            
            create_soap = self._create_account_soap_request(wex_data, credentials)
            create_response = await self._make_soap_request(create_soap, self.create_headers)
            print(f"Account creation response: {create_response}")
            
            account_token, description = self._process_creation_response(create_response)
            if not account_token or account_token == "":
                self._log_response(str(account_data.id), False, error=description)
                return self._create_error_response(
                    str(account_data.id),
                    description,
                    "WEX_CREATION_FAILED"
                )
            
            self.logger.info(f"WEX account created with token: {account_token}")
            await asyncio.sleep(self.creation_delay)
            
            for attempt in range(self.max_retries):
                try:
                    retrieve_soap = self._create_retrieval_soap_request(account_token, credentials)
                    retrieve_response = await self._make_soap_request(retrieve_soap, self.retrieve_headers)
                    print(f"Card retrieval response: {retrieve_response}")
                    
                    wex_response = self._process_retrieval_response(retrieve_response)
                    
                    if wex_response.success and wex_response.card_number:
                        card_number_display = None
                        if wex_response.card_number:
                            last_four = (
                                wex_response.card_number[-4:]
                                if len(wex_response.card_number) >= 4
                                else wex_response.card_number
                            )
                            card_number_display = f"XXXX-XXXX-XXXX-{last_four}"

                        # Success - return card details
                        self._log_response(
                            str(account_data.id), 
                            True, 
                            card_number_display=card_number_display
                        )
                        
                        return self._create_success_response(
                            str(account_data.id),
                            wex_response.card_number or "",
                            wex_response.expiry_date or "",
                            wex_response.cvc or "",
                            account_token,
                            {
                                "credit_rating": wex_response.credit_rating,
                                "status_code": wex_response.status_code,
                                "description": wex_response.description
                            }
                        )
                    
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Retrieval attempt {attempt + 1} failed, retrying...")
                        await asyncio.sleep(self.retry_delay)
                    else:
                        error_msg = f"Failed to retrieve card data after {self.max_retries} attempts: {wex_response.description}"
                        self._log_response(str(account_data.id), False, error=error_msg)
                        return self._create_error_response(
                            str(account_data.id),
                            error_msg,
                            "WEX_RETRIEVAL_FAILED"
                        )
                
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Retrieval attempt {attempt + 1} error: {str(e)}, retrying...")
                        await asyncio.sleep(self.retry_delay)
                    else:
                        error_msg = f"Error retrieving card data after {self.max_retries} attempts: {str(e)}"
                        self._log_response(str(account_data.id), False, error=error_msg)
                        return self._create_error_response(
                            str(account_data.id),
                            error_msg,
                            "WEX_RETRIEVAL_ERROR"
                        )
            
            # This should not be reached due to retry logic, but added for type safety
            error_msg = "Card retrieval failed - exhausted all retry attempts"
            self._log_response(str(account_data.id), False, error=error_msg)
            return self._create_error_response(
                str(account_data.id),
                error_msg,
                "WEX_RETRIEVAL_EXHAUSTED"
            )
        
        except Exception as e:
            error_msg = f"Unexpected error during WEX card creation: {str(e)}"
            self._log_response(str(account_data.id), False, error=error_msg)
            return self._create_error_response(
                str(account_data.id),
                error_msg,
                "WEX_UNEXPECTED_ERROR"
            )
