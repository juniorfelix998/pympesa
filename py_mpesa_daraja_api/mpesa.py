"""
Mpesa API Client

This module provides a Python client for interacting with the Safaricom Mpesa API. It handles
authentication, token management, and various Mpesa transactions such as B2B payments, B2C payments,
C2B transactions, transaction status requests, account balance requests, reversal requests, and
Lipa Na Mpesa Online payments.

Classes:
    Mpesa

Functions:
    oauth_generate_token
    encode_string
    current_timestamp
    convert_datetime_to_int

Dependencies:
    - requests
    - datetime
    - pydantic
    - logging
    - mpesa_urls
    - auth
    - mpesa_models
"""

import logging
import requests
from datetime import datetime, timedelta

from typing import Any, Dict, Optional, Tuple, Union, Type
from pydantic import BaseModel, ValidationError

from py_mpesa_daraja_api.mpesa_urls import MpesaURLs
from py_mpesa_daraja_api.auth import oauth_generate_token

from py_mpesa_daraja_api.mpesa_models import (
    B2BPaymentRequest,
    B2CPaymentRequest,
    C2BRegisterURL,
    C2BSimulateTransaction,
    TransactionStatusRequest,
    AccountBalanceRequest,
    ReversalRequest,
    LipaNaMpesaOnlineQuery,
    LipaNaMpesaOnlinePayment,
)

logger = logging.getLogger(__name__)


class Mpesa:
    """
    Mpesa API Client.

    This class provides methods to interact with the Mpesa API for various transaction types. It
    manages OAuth token generation and ensures tokens are refreshed when expired.

    Attributes:
        consumer_key (str): The consumer key for the Mpesa API.
        consumer_secret (str): The consumer secret for the Mpesa API.
        env (str): The environment for the Mpesa API ('sandbox' or 'production').
        version (str): The API version (default: 'v1').
        timeout (Optional[int]): The timeout for API requests (default: None).

    Methods:
        b2b_payment_request(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        b2c_payment_request(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        c2b_register_url(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        c2b_simulate_transaction(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        transaction_status_request(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        account_balance_request(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        reversal_request(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        lipa_na_mpesa_online_query(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
        lipa_na_mpesa_online_payment(data: Dict[str, Any]) -> Tuple[Union[Dict[str, Any], str], int]
    """

    _access_token: Optional[str] = None
    _token_expiry: Optional[datetime] = None

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        env: str,
        version: str = "v1",
        timeout: Optional[int] = None,
    ):
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.env = env
        self.version = version
        self.timeout = timeout
        self.headers = {}
        self.urls = MpesaURLs(env)
        if self._access_token is None or self._is_token_expired():
            self._set_access_token()
        self.headers = {"Authorization": f"Bearer {self._access_token}"}

    def _set_access_token(self) -> None:
        """
        Set the OAuth access token.

        This method retrieves a new OAuth access token using the provided consumer key and secret,
        and sets the token along with its expiry time.
        """
        token_data, _ = oauth_generate_token(
            self.consumer_key, self.consumer_secret, env=self.env
        )
        if token_data and "access_token" in token_data:
            Mpesa._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            Mpesa._token_expiry = datetime.now() + timedelta(seconds=int(expires_in))
            self.headers = {"Authorization": f"Bearer {self._access_token}"}
        else:
            logger.error("Failed to obtain access token")
            raise Exception("Failed to obtain access token")

    @classmethod
    def _is_token_expired(cls) -> bool:
        """
        Check if the current OAuth token is expired.

        :return: True if the token is expired or not set, False otherwise.
        """
        return cls._token_expiry is None or datetime.now() >= cls._token_expiry

    def _ensure_valid_token(self) -> None:
        """
        Ensure the OAuth token is valid.

        If the token is expired, this method will refresh it.
        """
        if self._is_token_expired():
            self._set_access_token()

    def _make_request(
        self, url: str, payload: Dict[str, Any], method: str
    ) -> Optional[requests.Response]:
        """
        Make an API request to the Mpesa server.

        :param url: The API endpoint URL.
        :param payload: The payload data for the request.
        :param method: The HTTP method (e.g., 'POST').
        :return: The response object if the request is successful, None otherwise.
        """
        self._ensure_valid_token()
        try:
            req = requests.request(
                method, url, headers=self.headers, json=payload, timeout=self.timeout
            )
            req.raise_for_status()
            return req
        except requests.RequestException as e:
            logger.exception(f"Error in {url} request. {str(e)}")
            return None

    def _process_and_request(
        self,
        data: Dict[str, Any],
        model: Type[BaseModel],
        url_func: callable,
        method: str = "POST",
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        """
        Process the request data, validate it, and make the API request.

        :param data: The request data.
        :param model: The Pydantic model for validation.
        :param url_func: The function to get the endpoint URL.
        :param method: The HTTP method (default: 'POST').
        :return: A tuple containing the response data and status code.
        """
        try:
            payload = model(**data).dict()
        except ValidationError as e:
            logger.error(f"Validation error: {e.json()}")
            return {"message": "Validation error"}, 400

        url = url_func()
        req = self._make_request(url, payload, method)

        if req is None:
            return {"message": "Request failed"}, 500

        response = req.json()
        if req.status_code == 401 and response.get("errorCode") == "404.001.03":
            self._set_access_token()
            req = self._make_request(url, payload, method)
            if req is None:
                return {"message": "Request failed after token refresh"}, 500
            response = req.json()

        return response, req.status_code

    def b2b_payment_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        """
        B2B Payment request.

        Initiates a B2B payment transaction from one company to another.

        API Endpoint:
            POST /mpesa/b2b/v1/paymentrequest

        :param data: A dictionary containing the request data. It should include the following keys:
            - 'Initiator': The name of the initiator.
            - 'SecurityCredential': The security credential.
            - 'CommandID': The command ID. (e.g., 'BusinessPayBill', 'BusinessBuyGoods', etc.)
            - 'SenderIdentifierType': The type of identifier for the sender. (e.g., '4' for shortcode)
            - 'ReceiverIdentifierType': The type of identifier for the receiver. (e.g., '4' for shortcode)
            - 'Amount': The transaction amount.
            - 'PartyA': The sender's shortcode.
            - 'PartyB': The receiver's shortcode.
            - 'Remarks': Any remarks for the transaction.
            - 'QueueTimeOutURL': The URL to receive a timeout response.
            - 'ResultURL': The URL to receive the result of the transaction.

        :return: A tuple containing the response data and status code.
            - If the request is successful, the response data will be a dictionary containing transaction details.
            - If the request fails, the response data will be a dictionary with a message indicating the failure reason.
            - The status code indicates the success (200) or failure (4xx or 5xx) of the request.
        """
        return self._process_and_request(
            data, B2BPaymentRequest, self.urls.get_b2b_payment_request_url
        )

    def b2c_payment_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        """
        B2C Payment Request.

        Initiates a B2C payment transaction from a company to a client.

        API Endpoint:
            POST /mpesa/b2c/v1/paymentrequest

        :param data: A dictionary containing the request data. It should include the following keys:
            - 'InitiatorName': The name of the initiator.
            - 'SecurityCredential': The security credential.
            - 'CommandID': The command ID. (e.g., 'BusinessPayment', 'SalaryPayment', etc.)
            - 'Amount': The transaction amount.
            - 'PartyA': The shortcode initiating the transaction.
            - 'PartyB': The recipient's phone number.
            - 'Remarks': Any remarks for the transaction.
            - 'QueueTimeOutURL': The URL to receive a timeout response.
            - 'ResultURL': The URL to receive the result of the transaction.
            - 'Occasion': The occasion for the transaction.

        :return: A tuple containing the response data and status code.
            - If the request is successful, the response data will be a dictionary containing transaction details.
            - If the request fails, the response data will be a dictionary with a message indicating the failure reason.
            - The status code indicates the success (200) or failure (4xx or 5xx) of the request.
        """
        return self._process_and_request(
            data, B2CPaymentRequest, self.urls.get_b2c_payment_request_url
        )

    def c2b_register_url(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, C2BRegisterURL, self.urls.get_c2b_register_url
        )

    def c2b_simulate_transaction(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, C2BSimulateTransaction, self.urls.get_c2b_simulate_url
        )

    def transaction_status_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, TransactionStatusRequest, self.urls.get_transaction_status_url
        )

    def account_balance_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, AccountBalanceRequest, self.urls.get_account_balance_url
        )

    def reversal_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, ReversalRequest, self.urls.get_reversal_request_url
        )

    def lipa_na_mpesa_online_query(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        return self._process_and_request(
            data, LipaNaMpesaOnlineQuery, self.urls.get_stk_push_query_url
        )

    def lipa_na_mpesa_online_payment(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        """
        Lipa Na M-Pesa Online Payment.

        Initiates an online payment using STK Push for Lipa Na M-Pesa.

        API Endpoint:
            POST /mpesa/stkpush/v1/processrequest

        :param data: A dictionary containing the request data. It should include the following keys:
            - 'BusinessShortCode': The business shortcode.
            - 'Password': The password for the online access.
            - 'Amount': The transaction amount.
            - 'PartyA': The initiating shortcode.
            - 'PartyB': The business shortcode.
            - 'PhoneNumber': The customer's phone number.
            - 'CallBackURL': The URL to receive the callback response.
            - 'AccountReference': The reference for the transaction.
            - 'TransactionDesc': The description of the transaction.

        :return: A tuple containing the response data and status code.
            - If the request is successful, the response data will be a dictionary containing transaction details.
            - If the request fails, the response data will be a dictionary with a message indicating the failure reason.
            - The status code indicates the success (200) or failure (4xx or 5xx) of the request.
        """
        return self._process_and_request(
            data, LipaNaMpesaOnlinePayment, self.urls.get_stk_push_process_url
        )
