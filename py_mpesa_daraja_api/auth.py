import base64
import logging
import requests

from typing import Any, Dict, Optional, Tuple
from py_mpesa_daraja_api.mpesa_urls import MpesaURLs


logger = logging.getLogger(__name__)


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
        response = requests.get(
            url,
            params=dict(grant_type=grant_type),
            auth=(consumer_key, consumer_secret),
        )
        response.raise_for_status()
        return response.json(), response.status_code
    except requests.RequestException as e:
        logger.exception(f"Error in {url} request. {str(e)}")
        return None, None


def encode_password(shortcode: str, passkey: str, timestamp: str) -> str:
    """
    Generate and return a base64 encoded password for online access.
    """
    to_encode = f"{shortcode}{passkey}{timestamp}".encode("utf-8")
    return base64.b64encode(to_encode).decode("utf-8")
