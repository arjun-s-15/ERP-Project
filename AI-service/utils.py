import re
def clean_code(code: str) -> str:
    code = code.strip()
    
    code = re.sub(r'^```(?:\w+)?\s*\n?', '', code)
    code = re.sub(r'\n?```$', '', code)
    
    return code.strip()

canonical_feature_set = [
    {
        "name": "transaction_id",
        "description": "Unique identifier representing a sales transaction document such as an invoice or order.",
        "expected_dtype": "string",
        "required": True,
        "possible_source_names": [
        "invoice",
        "invoiceno",
        "orderid",
        "sales_id",
        "billno",
        "trx_no"
        ]
    },
    {
        "name": "item_id",
        "description": "Unique identifier representing the product or SKU sold in the transaction.",
        "expected_dtype": "string",
        "required": True,
        "possible_source_names": [
        "stockcode",
        "productid",
        "sku",
        "itemcode",
        "materialno"
        ]
    },
    {
        "name": "quantity",
        "description": "Number of units of the product sold in the transaction.",
        "expected_dtype": "numeric",
        "required": True,
        "constraints": {
        "must_be_numeric": True
        },
        "possible_source_names": [
        "qty",
        "quantity",
        "units",
        "unitssold",
        "qty_sold",
        "amount"
        ]
    },
    {
        "name": "transaction_timestamp",
        "description": "Timestamp representing when the sales transaction occurred.",
        "expected_dtype": "datetime",
        "required": True,
        "constraints": {
        "must_be_datetime_compatible": True
        },
        "possible_source_names": [
        "invoicedate",
        "ordertime",
        "transactiondate",
        "timestamp",
        "datetime",
        "salesdate"
        ]
    },
    {
        "name": "unit_price",
        "description": "Price per unit of the product sold in the transaction.",
        "expected_dtype": "numeric",
        "required": False,
        "constraints": {
        "must_be_numeric": True,
        "must_be_positive": True
        },
        "possible_source_names": [
        "price",
        "unitprice",
        "sellingprice",
        "rate",
        "cost",
        "amount"
        ]
    },
    {
        "name": "customer_id",
        "description": "Unique identifier representing the customer who made the purchase.",
        "expected_dtype": "string",
        "required": False,
        "possible_source_names": [
        "customerid",
        "client",
        "buyerid",
        "accountno",
        "partycode"
        ]
    },
    {
        "name": "region",
        "description": "Geographic region or territory where the sale occurred.",
        "expected_dtype": "string",
        "required": False,
        "possible_source_names": [
        "country",
        "region",
        "state",
        "zone",
        "market"
        ]
    }
]