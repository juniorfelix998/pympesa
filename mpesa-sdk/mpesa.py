import logging
import base64
from typing import Any, Dict, Optional, Tuple, Union, Type
from pydantic import BaseModel, ValidationError
import requests
from mpesa_urls import MpesaURLs
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

logger = logging.getLogger()


class Mpesa:
    def __init__(
        self,
        access_token: str,
        env: str,
        version: str = "v1",
        timeout: Optional[int] = None,
    ):
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.urls = MpesaURLs(env)
        self.version = version
        self.timeout = timeout

    def make_request(
        self, url: str, payload: Dict[str, Any], method: str
    ) -> Optional[requests.Response]:
        """
        Invoke URL and return a response object.
        """
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
        expected_keys: Type[BaseModel],
        url_func: callable,
        method: str = "POST",
    ) -> Tuple[Union[Dict[str, Any], str], int]:
        """
        Process data and make a request.
        """
        try:
            payload = expected_keys(**data).dict()
        except ValidationError as e:
            logger.error(f"Validation error: {e.json()}")
            return {"message": "Validation error"}, 400

        url = url_func()
        req = self.make_request(url, payload, method)
        return (
            req.json() if req else {"message": "Request failed"},
            req.status_code if req else 500,
        )

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


def oauth_generate_token(
    consumer_key: str,
    consumer_secret: str,
    grant_type: str = "client_credentials",
    env: str = "sandbox",
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    """
    Authenticate your app and return an OAuth access token.
    """
    urls = MpesaURLs(env)
    url = urls.get_generate_token_url()
    try:
        req = requests.get(
            url,
            params=dict(grant_type=grant_type),
            auth=(consumer_key, consumer_secret),
        )
        req.raise_for_status()
        return req.json(), req.status_code
    except requests.RequestException as e:
        logger.exception(f"Error in {url} request. {str(e)}")
        return None, None


def encode_password(shortcode: str, passkey: str, timestamp: str) -> str:
    """
    Generate and return a base64 encoded password for online access.
    """
    to_encode = f"{shortcode}{passkey}{timestamp}".encode("utf-8")
    return base64.b64encode(to_encode).decode("utf-8")
