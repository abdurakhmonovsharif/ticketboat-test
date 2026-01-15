from enum import Enum
from typing import Literal, Annotated, Optional, List, Dict, Any
from uuid import UUID

import pytz
from pydantic import BaseModel, Field, field_validator


class PersonRequestModel(BaseModel):
    first_name: str
    last_name: str
    date_of_birth: Optional[str]
    status: Annotated[str, Literal["Active", "Inactive", "Active - No New Accts"]]

    name_quality: Annotated[str, Literal["Employee", "Non-Employee", "Contractor", "Department"]]
    last_4_ssn: Optional[str]
    notes: Optional[str]
    full_name: Optional[str]


class AddressRequestModel(BaseModel):
    metro_area_id: Optional[str]
    street_one: str
    street_two: Optional[str]
    city: str
    state_id: str
    postal_code: str
    notes: Optional[str]
    address_type: Annotated[str, Literal['Account Address', 'Billing Address']]
    address_name: Optional[str]
    shippable: bool = Field(default=False)


class MainFilters(BaseModel):
    page: int
    page_size: int
    sort_field: Optional[str] = None
    sort_order: Optional[str] = 'desc'
    search_query: str
    timezone: Optional[str] = Field("America/Chicago",
                                    description="Timezone name (must be a valid IANA timezone like 'Asia/Tashkent')")

    @field_validator("timezone")
    def validate_timezone(celf, value):
        if value is None:
            return "America/Chicago"

        if value not in pytz.all_timezones:
            raise ValueError(f"Invalid timezone: {value}")
        return value


class FilteredAddressRequestModel(MainFilters):
    metro_area_ids: Optional[List[str]] = None
    assigned_to_account: Literal["all", "yes", "no"]
    address_type: Optional[Annotated[str, Literal['Account Address', 'Billing Address']]] = None


class FilteredAccountsRequestModel(MainFilters):
    metro_area_ids: Optional[List[str]] = None
    company_ids: Optional[List[str]] = None
    created_at: Optional[str] = None
    incomplete_steps: Optional[List[str]] = None
    status_code: Optional[List[str]] = None
    address_search_query: Optional[str] = None


class FilteredAccountsCSVRequestModel(BaseModel):
    sort_field: Optional[str] = None
    sort_order: Optional[str] = 'desc'
    search_query: str = ""
    metro_area_ids: Optional[List[str]] = None
    company_ids: Optional[List[str]] = None
    created_at: Optional[str] = None
    incomplete_steps: Optional[List[str]] = None
    status_code: Optional[List[str]] = None
    address_search_query: Optional[str] = None


class FilteredProxiesRequestModel(MainFilters):
    metro_area_ids: Optional[List[str]] = None
    provider_id: str
    status: Optional[Annotated[str, Literal['Available', 'In-Use', 'Replaced', 'Retired']]] = Field(
        default=None,
        description="Filter proxies by status"
    ),


class FilteredCCsRequestModel(MainFilters):
    metro_area_ids: Optional[List[str]] = None
    company_ids: Optional[List[str]] = None


class EmailRequestModel(BaseModel):
    ams_person_id: str
    created_by: str
    email_address: str
    password: str
    status: Annotated[str, Literal['AVAILABLE', 'IN USE', 'SUSPENDED', 'RETIRED', 'In Use - Mgmt']]
    recovery_email_ids: Optional[list[str]] = None
    recovery_phone_ids: Optional[list[str]] = None
    pva_phone_id: Optional[str]
    robot_check_phone: Optional[str]
    backup_codes: Optional[str]
    paid_account: bool = Field(default=False)
    spam_filter_setup_completed: bool = Field(default=False)
    catchall_forward_setup_completed: bool = Field(default=False)
    update_gmail_name: bool = Field(default=False)
    gmail_filter_forwarding_setup: bool = Field(default=False)
    recovery_email_setup: bool = Field(default=False)
    recovery_phone_setup: bool = Field(default=False)
    send_testing_email: bool = Field(default=False)
    notes: Optional[str]
    forwarding_email_ids: Optional[list[str]] = None
    catchall_email_id: Optional[str] = None


class UpdatedFields(BaseModel):
    status: Optional[str] = None
    paid_account: Optional[bool] = None
    catchall_forward_setup_completed: Optional[bool] = None
    spam_filter_setup_completed: Optional[bool] = None
    update_gmail_name: Optional[bool] = None
    gmail_filter_forwarding_setup: Optional[bool] = None
    recovery_email_setup: Optional[bool] = None
    recovery_phone_setup: Optional[bool] = None
    send_testing_email: Optional[bool] = None
    recovery_email_ids: Optional[List[str]] = None
    recovery_phone_ids: Optional[List[str]] = None
    forwarding_email_ids: Optional[List[str]] = None
    catchall_email_id: Optional[str] = None


class EmailCommonFieldsRequest(BaseModel):
    email_ids: list[str]
    updated_fields: UpdatedFields


class AcctStepBaseModel(BaseModel):
    id: str
    account_id: str
    step_id: str
    time_completed: str


class AcctStepRequestModel(AcctStepBaseModel):
    account_id: str


class AccountRequestModel(BaseModel):
    nickname: str
    ams_person_id: str
    ams_email_id: Optional[str]
    ams_address_id: str
    ams_proxy_id: Optional[str] = None
    phone_number_id: str
    company_id: str
    completed_steps: Optional[List[AcctStepBaseModel]] = None
    notes: Optional[str]


class AccountRequestModelV2(BaseModel):
    nickname: str
    company_id: str
    ams_person_id: str
    ams_address_id: str
    ams_email_id: Optional[str] = None
    phone_number_id: Optional[str] = None
    ams_proxy_id: Optional[str] = None
    tags: Optional[List[str]] = None
    automator: Optional[str] = None
    point_of_sale: Optional[str] = None


class RebuildAccountRequestItem(BaseModel):
    old_account_id: str
    new_nickname: str
    company_id: Optional[str] = None
    ams_person_id: Optional[str] = None
    ams_address_id: Optional[str] = None
    phone_number_id: Optional[str] = None
    ams_proxy_id: Optional[str] = None
    automator_id: Optional[str] = None
    point_of_sale_id: Optional[str] = None
    email_id: Optional[str] = None
    tags: Optional[List[str]] = None


class RebuildAccountsRequest(BaseModel):
    accounts: List[RebuildAccountRequestItem]


class AccountRequestPayloadV2Model(BaseModel):
    create_mlx_profile: bool = False
    accounts: List[AccountRequestModelV2]


class PhoneNumberRequestModel(BaseModel):
    number: str
    provider_code: Optional[str]
    created_at: Optional[str]
    created_by: str
    status: Annotated[str, Literal['Active', 'Cancelled', 'Active-NoPhone', 'Special']]
    notes: Optional[str]
    account_id: Optional[str] = Field(default=None, exclude=True)

    class Config:
        model_config = {
            "populate_by_name": True,
            "extra": "ignore"
        }


class PhoneProviderType(str, Enum):
    """Supported phone provider types"""
    PHYSICAL_PHONE = "Physical Phone"
    TEXT_ONLY = "Text Only"


class CreditCardCreateRequest(BaseModel):
    ams_account_id: Optional[str] = None
    card_type: Optional[str] = None
    issuer_id: Optional[str] = None
    card_number: str
    expiration_month: int = Field(..., ge=1, le=12)
    expiration_year: int = Field(..., ge=2000, le=2100)
    cvv: str
    ams_person_id: str
    account_address_id: Optional[str] = None
    avs_address_id: Optional[str] = None
    avs_same_as_account: Optional[bool] = False
    company_id: str
    tm_card: Optional[bool] = False
    status: Annotated[str, Literal['active', 'inactive']]
    type: Annotated[str, Literal['Physical Card', 'Virtual Card']]
    secondary_card: Optional[bool] = False
    nickname: Optional[str] = None
    created: str
    created_by: str


class CreditCardUpdateRequest(BaseModel):
    card_ids: List[str] = Field(default_factory=list)
    company: Optional[str] = None
    assignment: Optional[bool] = None
    tm: Optional[bool] = None
    status: Optional[bool] = None
    nickname: Optional[str] = None


class CreditCardSingleUpdateRequest(BaseModel):
    ams_account_id: Optional[str] = None
    ams_person_id: Optional[str] = None
    card_number: Optional[str] = None
    cvv: Optional[str] = None
    card_type: Optional[str] = None
    issuer_id: Optional[str] = None
    expiration_month: Optional[int] = None
    expiration_year: Optional[int] = None
    tm_card: Optional[bool] = None
    status: Optional[str] = None
    type: Optional[str] = None
    secondary_card: Optional[bool] = None
    nickname: Optional[str] = None
    account_address_id: Optional[str] = None
    avs_address_id: Optional[str] = None
    avs_same_as_account: Optional[bool] = False


class CreditCardUpdateResult(BaseModel):
    updated: List[str]
    not_found: List[str]


class State(BaseModel):
    id: UUID
    name: Optional[str]
    abbreviation: Optional[str]


class Address(BaseModel):
    id: UUID
    street_one: Optional[str] = None
    street_two: Optional[str] = None
    postal_code: Optional[str] = None
    city: Optional[str] = None
    state: Optional[State] = None


class CreditCard(BaseModel):
    id: UUID
    account: Optional[str] = None
    nickname: Optional[str] = None
    card_number: Optional[str] = None
    card_type: Optional[str] = None
    issuer: Optional[str] = None
    expires: Optional[str] = None
    cvv: Optional[str] = None
    created: Optional[str] = None
    account_nickname: Optional[str] = None
    cardholder_name: Optional[str] = None
    address: Optional[Address] = None
    tm_card: Optional[bool] = None
    company: Optional[str] = None
    status: Optional[str] = None
    starred: Optional[bool] = None


class EncryptedCreditCardDataWithKeyId(BaseModel):
    encrypted_data: str
    encrypted_key_id: str


class EncryptionKeyResponse(BaseModel):
    """Response model for an encryption key"""
    key_id: str
    encryption_key: str
    expires_at: str


class ProxyRequestModel(BaseModel):
    proxy: str
    zone: str
    provider_id: str
    proxy_metro: Optional[str]
    notes: Optional[str]
    ams_account_id: Optional[str]
    status_code: str


class SyncStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    SKIPPED = "skipped"


class Primary(BaseModel):
    id: str
    primary_name: Optional[str]
    primary_code: Optional[str]
    password: Optional[str]
    ticketsuite_persona_id: Optional[str]
    is_juiced: bool
    added_to_ts: bool
    missing_fields: List[str]


class AccountPrimary(BaseModel):
    account_id: str
    account_nickname: Optional[str]
    is_shadows: bool
    email_id: Optional[str]
    email_address: Optional[str]
    proxy: Optional[dict]
    phone: Optional[dict]
    primaries: List[Primary]


class AddPrimaryAccountMappingRequest(BaseModel):
    primary_id: str
    password: Optional[str] = None
    generate_password: bool = False
    

class UpdatePrimaryAccountMappingRequest(BaseModel):
    primary_id: str
    password: Optional[str] = None


class CreateTicketSuitePersonasRequest(BaseModel):
    """Request model for creating TicketSuite personas"""
    account_ids: List[str] = Field(..., description="List of account IDs to create personas for")


class AccountPrimaryPayload(BaseModel):
    """Single account with its primary IDs for persona creation"""
    account_id: str = Field(..., description="Account ID to create personas for")
    primary_ids: List[str] = Field(..., description="List of primary IDs to sync for this account")


class SyncResult(BaseModel):
    primary_name: Optional[str]
    status: SyncStatus
    status_code: Optional[int]
    error: Optional[str]
    response: Optional[dict]


class AccountSyncResults(BaseModel):
    account_id: str
    email_address: Optional[str]
    sync_results: List[SyncResult]


class TicketSuiteProxy(BaseModel):
    Host: str
    Port: int
    Username: str
    Password: str


class TicketSuitePhone(BaseModel):
    PhoneNumber: str
    Provider: str
    IsRotation: bool
    IsEnabled: bool


class TicketSuitePersonaPayload(BaseModel):
    Email: str
    Password: Optional[str] = None
    Tags: str
    InternalNotes: Optional[str] = ''
    RoleTags: Optional[str] = ''
    AccessToken: Optional[str] = ''
    StockTypePriority: Optional[str] = ''
    Proxy: Optional[TicketSuiteProxy] = None
    SyncToAxsResale: Optional[bool] = False
    SyncToTradedesk: Optional[bool] = False
    InventoryTags: Optional[str] = ''
    PhoneNumber: Optional[TicketSuitePhone] = None
    PosVendorId: Optional[str] = ''
    email_id: Optional[str] = ''
    primary_id: Optional[str] = ''
    automator_id: Optional[str] = ''
    account_id: Optional[str] = ''

    def exclude_before_request(self):
        """Exclude internal tracking fields before sending request"""
        return self.model_dump(exclude={'email_id', 'primary_id', 'automator_id', 'account_id'})


class UpdateStageOrder(BaseModel):
    stage_id: str
    new_order_index: int


class UpdateStageLink(BaseModel):
    stage_id: str
    stage_link: Optional[str]


class UpdateStepOrder(BaseModel):
    step_id: str
    new_order_index: int


class UpdateStep(BaseModel):
    name: str
    type: str
    api_details: Optional[str] = None


class UpdateStepDependencies(BaseModel):
    prerequisite_step_ids: list[str]


class EmailTwoFARequestModel(BaseModel):
    type: Annotated[str, Literal["Phone", "Passkey", "Authenticator"]]
    value: str
    active: bool = Field(default=False)


class EmailTwoFAResponseModel(BaseModel):
    id: str
    email_id: str
    type: str
    value: str
    active: bool
    created_at: Optional[str] = None
    last_modified: Optional[str] = None


class CreateStep(BaseModel):
    api_details: Optional[str] = None
    created_at: str
    id: str
    name: str
    order_index: int
    stage_id: str
    type: Annotated[str, Literal['manual', 'automatic']]
    updated_at: str


class CreditCardProvider(str, Enum):
    """Supported credit card providers"""
    WEX = "wex"
    DIVVY = "divvy"
    AMEX = "amex"
    CITI = "citi"
    GLOBAL_REWARDS = "global rewards"
    CORPAY = "corpay"


class CreateCreditCardIssuerRequest(BaseModel):
    label: str = Field(..., description="Name of the credit card issuer")
    has_avs: bool = Field(..., description="Whether the issuer supports AVS")


class OrderCreditCardRequest(BaseModel):
    account_id: UUID = Field(..., description="AMS account ID to order card for")
    provider: CreditCardProvider = Field(..., description="Credit card provider")
    credit_limit: float = Field(..., gt=0, description="Credit limit for the card")
    nickname: Optional[str] = Field(default=None, description="Optional nickname for the credit card")
    additional_params: Optional[Dict[str, Any]] = Field(default=None, description="Provider-specific parameters")


class OrderCreditCardResponse(BaseModel):
    success: bool = Field(..., description="Whether the card ordering was successful")
    provider: CreditCardProvider = Field(..., description="Credit card provider used")
    account_id: UUID = Field(..., description="AMS account ID")
    card_number: Optional[str] = Field(default=None, description="Credit card number")
    expiry_date: Optional[str] = Field(default=None, description="Card expiry date")
    cvc: Optional[str] = Field(default=None, description="Card CVC/CVV")
    account_token: Optional[str] = Field(default=None, description="Provider account token")
    error_message: Optional[str] = Field(default=None, description="Error message if ordering failed")
    error_code: Optional[str] = Field(default=None, description="Provider-specific error code")
    provider_data: Optional[Dict[str, Any]] = Field(default=None,
                                                    description="Additional provider-specific response data")


class CreditCardOrderItem(BaseModel):
    provider: CreditCardProvider = Field(..., description="Credit card provider")
    credit_limit: float = Field(..., gt=0, description="Credit limit for the card")
    nickname: Optional[str] = Field(default=None, description="Optional nickname for the credit card")
    additional_params: Optional[Dict[str, Any]] = Field(default=None, description="Provider-specific parameters")


class AccountCreditCardOrders(BaseModel):
    account_id: UUID = Field(..., description="AMS account ID")
    orders: List[CreditCardOrderItem] = Field(..., description="List of credit card orders for this account")


class BulkOrderCreditCardResponse(BaseModel):
    overall_success: bool = Field(..., description="Whether all card orders were successful")
    results: List[OrderCreditCardResponse] = Field(..., description="Individual card order results")
    total_requested: int = Field(..., description="Total number of cards requested")
    total_successful: int = Field(..., description="Total number of cards successfully created")
    total_failed: int = Field(..., description="Total number of cards that failed to create")


class AccountData(BaseModel):
    id: UUID
    nickname: Optional[str]
    company_id: UUID
    company_name: Optional[str]
    person_id: UUID
    person_first_name: str
    person_last_name: str
    person_full_name: Optional[str]
    address_street_one: str
    address_street_two: Optional[str]
    address_city: str
    address_state: str
    address_postal_code: str
    address_country: str
    email_address: Optional[str]
    phone_number: Optional[str]
