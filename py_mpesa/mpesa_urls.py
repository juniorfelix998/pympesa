SANDBOX_BASE_URL = "https://sandbox.safaricom.co.ke"
PROD_BASE_URL = "https://api.safaricom.co.ke"


class MpesaURLs:
    def __init__(self, env: str = "sandbox"):
        self.base_url = SANDBOX_BASE_URL if env == "sandbox" else PROD_BASE_URL

    def get_generate_token_url(self) -> str:
        return f"{self.base_url}/oauth/v1/generate"

    def get_reversal_request_url(self) -> str:
        return f"{self.base_url}/mpesa/reversal/v1/request"

    def get_b2c_payment_request_url(self) -> str:
        return f"{self.base_url}/mpesa/b2c/v1/paymentrequest"

    def get_b2b_payment_request_url(self) -> str:
        return f"{self.base_url}/mpesa/b2b/v1/paymentrequest"

    def get_c2b_register_url(self) -> str:
        return f"{self.base_url}/mpesa/c2b/v1/registerurl"

    def get_c2b_simulate_url(self) -> str:
        return f"{self.base_url}/mpesa/c2b/v1/simulate"

    def get_transaction_status_url(self) -> str:
        return f"{self.base_url}/mpesa/transactionstatus/v1/query"

    def get_account_balance_url(self) -> str:
        return f"{self.base_url}/mpesa/accountbalance/v1/query"

    def get_stk_push_query_url(self) -> str:
        return f"{self.base_url}/mpesa/stkpushquery/v1/query"

    def get_stk_push_process_url(self) -> str:
        return f"{self.base_url}/mpesa/stkpush/v1/processrequest"
