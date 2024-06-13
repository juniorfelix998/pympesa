**M-pesa SDK**

The M-Pesa SDK is a Python library that provides an easy-to-use interface for 
integrating M-Pesa APIs into your Python applications. It simplifies the process 
of initiating transactions, querying transaction status, and performing other M-Pesa operations.
[MPESA API](https://developer.safaricom.co.ke/docs#authentication).

**Features**

* Initiate B2B (Business to Business) transactions
* Initiate B2C (Business to Customer) transactions
* Register validation and confirmation URLs for C2B (Customer to Business) transactions
* Simulate C2B transactions
* Check transaction status
* Inquire account balance
* Perform transaction reversal
* Initiate Lipa Na M-Pesa Online payments using STK Push

***Installation***

You can install the M-Pesa SDK via pip:
   ```bash
   pip install py_mpesa_daraja_api
   ```


***Usage***
```python
from py_mpesa_daraja_api.mpesa import Mpesa

# Initialize Mpesa instance
mpesa = Mpesa(consumer_key='YOUR_CONSUMER_KEY', consumer_secret='YOUR_CONSUMER_SECRET', env='sandbox')

# Example: Initiate B2C Payment Request
data = {
    'InitiatorName': 'test',
    'SecurityCredential': 'security_credential',
    'CommandID': 'SalaryPayment',
    'Amount': 1000,
    'PartyA': 'shortcode',
    'PartyB': 'customer_phone',
    'Remarks': 'Salary payment',
    'QueueTimeOutURL': 'timeout_url',
    'ResultURL': 'result_url',
    'Occasion': 'Salary Payment'
}
response, status_code = mpesa.b2c_payment_request(data)
print(response, status_code)
```

