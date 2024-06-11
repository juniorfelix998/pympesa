from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


def generate_timestamp() -> str:
    """
    Return the current timestamp formatted as YYYYMMDDHHMMSS
    """
    return datetime.now().strftime("%Y%m%d%H%M%S")


class B2BPaymentRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    CommandID: str
    SenderIdentifierType: int
    RecieverIdentifierType: int
    Amount: float
    PartyA: str
    PartyB: str
    AccountReference: str
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str


class B2CPaymentRequest(BaseModel):
    InitiatorName: str
    SecurityCredential: str
    CommandID: str
    Amount: float
    PartyA: str
    PartyB: str
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    Occasion: Optional[str]


class C2BRegisterURL(BaseModel):
    ShortCode: str
    ResponseType: str
    ConfirmationURL: str
    ValidationURL: str


class C2BSimulateTransaction(BaseModel):
    ShortCode: str
    Amount: float
    Msisdn: str
    CommandID: str = "CustomerPayBillOnline"


class TransactionStatusRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    TransactionID: str
    PartyA: str
    ResultURL: str
    QueueTimeOutURL: str
    Remarks: str
    Occasion: Optional[str]
    CommandID: str = "TransactionStatusQuery"
    IdentifierType: int = 1


class AccountBalanceRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    PartyA: str
    Remarks: str
    QueueTimeOutURL: str
    ResultURL: str
    CommandID: str = "AccountBalance"
    IdentifierType: int = 4


class ReversalRequest(BaseModel):
    Initiator: str
    SecurityCredential: str
    TransactionID: str
    Amount: float
    ReceiverParty: str
    ResultURL: str
    QueueTimeOutURL: str
    Remarks: str
    Occasion: Optional[str]
    CommandID: str = "TransactionReversal"
    RecieverIdentifierType: int = 4


class LipaNaMpesaOnlineQuery(BaseModel):
    BusinessShortCode: str
    Password: str
    Timestamp: str = Field(default_factory=generate_timestamp)
    CheckoutRequestID: str


class LipaNaMpesaOnlinePayment(BaseModel):
    BusinessShortCode: str
    Password: str
    Amount: float
    PartyA: str
    PartyB: str
    PhoneNumber: str
    CallBackURL: str
    AccountReference: str
    TransactionDesc: str
    Timestamp: str = Field(default_factory=generate_timestamp)
    TransactionType: str = "CustomerPayBillOnline"
