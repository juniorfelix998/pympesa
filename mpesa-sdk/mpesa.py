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

from mpesa_urls import MpesaURLs
from auth import oauth_generate_token

from mpesa_models import (
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
        return cls._token_expiry is None or datetime.now() >= cls._token_expiry

    def _ensure_valid_token(self) -> None:
        if self._is_token_expired():
            self._set_access_token()

    def _make_request(
        self, url: str, payload: Dict[str, Any], method: str
    ) -> Optional[requests.Response]:
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
        return self._process_and_request(
            data, B2BPaymentRequest, self.urls.get_b2b_payment_request_url
        )

    def b2c_payment_request(
        self, data: Dict[str, Any]
    ) -> Tuple[Union[Dict[str, Any], str], int]:
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
        return self._process_and_request(
            data, LipaNaMpesaOnlinePayment, self.urls.get_stk_push_process_url
        )
