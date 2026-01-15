from typing import Optional, List
from pydantic import BaseModel, Field


class CorpayBillingAddress(BaseModel):
    addressLine1: Optional[str] = Field(default=None, description="Primary street address")
    addressLine2: Optional[str] = Field(default=None, description="Secondary address line")
    city: Optional[str] = Field(default=None, description="City")
    state: Optional[str] = Field(default=None, description="State or province")
    zipCode: Optional[str] = Field(default=None, description="Postal/ZIP code")
    country: Optional[str] = Field(default=None, description="Country code")


class CorpayIndividualControl(BaseModel):
    billingCycle: str = Field(..., description="Cycle granularity (e.g., Weekly)")
    billingCycleDay: Optional[str | int] = Field(default=None, description="Cycle anchor day")
    cycleTransactionCount: int = Field(..., ge=0, description="Max transactions per cycle")
    dailyAmountLimit: float = Field(..., ge=0, description="Max amount per day")
    dailyTransactionCount: int = Field(..., ge=0, description="Max transactions per day")
    transactionAmountLimit: float = Field(..., ge=0, description="Max amount per transaction")
    amount: Optional[float] = Field(default=None, ge=0, description="Optional overall amount limit")
    mcc: Optional[int] = Field(default=None, description="Merchant category code")
    open: Optional[bool] = Field(default=None, description="Whether the control is open")


class CorpayMccGroupControl(BaseModel):
    groupName: str = Field(..., description="Named MCC group")
    billingCycle: str = Field(..., description="Cycle granularity")
    billingCycleDay: Optional[str | int] = Field(default=None, description="Cycle anchor day")
    cycleTransactionCount: int = Field(..., ge=0, description="Max transactions per cycle")
    dailyAmountLimit: float = Field(..., ge=0, description="Max amount per day")
    dailyTransactionCount: int = Field(..., ge=0, description="Max transactions per day")
    transactionAmountLimit: float = Field(..., ge=0, description="Max amount per transaction")
    amount: Optional[float] = Field(default=None, ge=0, description="Optional overall amount limit")
    open: Optional[bool] = Field(default=None, description="Whether the control is open")


class CorpayMetaData(BaseModel):
    userDefinedField1: Optional[str] = Field(default=None, description="Custom metadata field 1")
    userDefinedField2: Optional[str] = Field(default=None, description="Custom metadata field 2")
    userDefinedField3: Optional[str] = Field(default=None, description="Custom metadata field 3")
    userDefinedField4: Optional[str] = Field(default=None, description="Custom metadata field 4")
    userDefinedField5: Optional[str] = Field(default=None, description="Custom metadata field 5")


class CorpayCardData(BaseModel):
    amount: float = Field(..., ge=0, description="Requested funding amount")
    billingAddress: CorpayBillingAddress = Field(..., description="Billing address data")
    emailAddress: str = Field(..., description="Cardholder email address")
    employeeNumber: str = Field(..., description="Employee identifier")
    firstName: str = Field(..., description="Cardholder first name")
    lastName: str = Field(..., description="Cardholder last name")
    mobilePhoneNumber: Optional[str] = Field(default=None, description="Mobile phone number")
    type: str = Field(..., description="Card type (e.g., Ghost)")
    setAlertServiceFlag: bool = Field(..., description="Whether alerts are enabled")
    individualControls: Optional[CorpayIndividualControl] = Field(
        default=None, description="Per-card control limits"
    )
    individualMccControls: Optional[List[CorpayIndividualControl]] = Field(
        default=None, description="Per-MCC individual controls"
    )
    mccGroupControls: Optional[List[CorpayMccGroupControl]] = Field(
        default=None, description="Controls by MCC group"
    )
    metaData: Optional[CorpayMetaData] = Field(
        default=None, description="Custom metadata for the card"
    )


class CorpayCustomerData(BaseModel):
    id: str = Field(..., description="Customer identifier")
    accountCode: str = Field(..., description="Customer account code")


class CorpayCreationData(BaseModel):
    card: CorpayCardData = Field(..., description="Card issuance payload")
    customer: CorpayCustomerData = Field(..., description="Customer context for the card")


class CorpayCardResponseData(CorpayCardData):
    createdDateTimestamp: str = Field(..., description="Card creation timestamp")
    cvc2: str = Field(..., description="Card verification code")
    number: str = Field(..., description="Issued card number")
    token: str = Field(..., description="Card token identifier")


class CorpayCreationResponse(BaseModel):
    card: CorpayCardResponseData = Field(..., description="Issued Corpay card data")
    customer: CorpayCustomerData = Field(..., description="Customer associated with the card")


class GlobalRewardsCardData(BaseModel):
    cardNumber: str = Field(..., description="Credit card number")
    expDate: str = Field(..., description="Expiration date in YYYYMM format")
    lastFour: str = Field(..., description="Last four digits of the card number")
    cvc: str = Field(..., description="Card verification code")


class GlobalRewardsCreationData(BaseModel):
    authorizationKey: str = Field(..., description="Authorization key for API access")
    firstName: str = Field(..., description="Person's first name")
    lastName: str = Field(..., description="Person's last name")
    address1: str = Field(..., description="Street address")
    address2: str = Field(default="", description="Second line for address (optional)")
    city: str = Field(..., description="City")
    state: str = Field(..., description="State abbreviation")
    postalCode: str = Field(..., description="ZIP/Postal code")
    clientId: Optional[str] = Field(default="", description="Client identifier")
    metaField1: str = Field(default="", description="Metadata field 1 (optional)")
    metaField2: str = Field(default="", description="Metadata field 2 (optional)")
    cardBin: str = Field(..., description="Card BIN number")
    monthlyLimit: float = Field(..., gt=0, description="Monthly credit limit")
    limitWindow: str = Field(..., description="Limit window duration")
    transactionLimit: Optional[float] = Field(default=None, gt=0, description="Per-transaction limit")
    cardBrand: Optional[str] = Field(default=None, description="Brand of the credit card")
    terminationDate: Optional[str] = Field(
        default=None, description="Card termination date (optional)"
    )


class GlobalRewardsResponse(BaseModel):
    clientId: str = Field(..., description="Client identifier")
    globalrewardsId: str = Field(..., description="Global Rewards account ID")
    cardDetails: GlobalRewardsCardData = Field(
        ..., description="Details of the issued credit card"
    )


class WEXAccountCreationData(BaseModel):
    last_name: str = Field(..., description="Person's last name")
    first_name: str = Field(..., description="Person's first name")
    name_line2: str = Field(default="", description="Second line for name (optional)")
    address_line1: str = Field(..., description="Street address")
    city: str = Field(..., description="City")
    state: str = Field(..., description="State abbreviation")
    zip: str = Field(..., description="ZIP/Postal code")
    country: str = Field(..., description="Country")
    credit_limit: float = Field(..., gt=0, description="Credit limit")


class WEXAccountResponse(BaseModel):
    account_token: Optional[str] = Field(default=None, description="WEX account token")
    description: str = Field(..., description="Response description")
    success: bool = Field(..., description="Whether creation was successful")
    card_number: Optional[str] = Field(default=None, description="Credit card number")
    expiry_date: Optional[str] = Field(default=None, description="Card expiry date")
    cvc: Optional[str] = Field(default=None, description="Card CVC")
    credit_rating: Optional[str] = Field(default=None, description="Credit rating")
    status_code: Optional[str] = Field(default=None, description="Account status code")
