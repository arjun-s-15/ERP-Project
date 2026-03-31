import re
def clean_code(code: str) -> str:
    code = code.strip()
    
    code = re.sub(r'^```(?:\w+)?\s*\n?', '', code)
    code = re.sub(r'\n?```$', '', code)
    
    return code.strip()

canonical_feature_set = [
    {
        "name": "event_id",
        "description": "Unique identifier for a sales record. Can represent a transaction line item or an aggregated sales record (e.g., store-item-date).",
        "expected_dtype": "string",
        "required": False,
        "generation_strategy": "concatenate_keys_if_missing",
        "possible_source_names": [
            "transaction_id",
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
        "description": "Unique identifier representing the product or SKU.",
        "expected_dtype": "string",
        "required": True,
        "possible_source_names": [
            "stockcode",
            "productid",
            "sku",
            "itemcode",
            "materialno",
            "item_nbr"
        ]
    },
    {
        "name": "location_id",
        "description": "Identifier for store, warehouse, or sales location.",
        "expected_dtype": "string",
        "required": False,
        "possible_source_names": [
            "store",
            "store_nbr",
            "shop_id",
            "location",
            "branch"
        ]
    },
    {
        "name": "quantity",
        "description": "Number of units sold. Can represent per-transaction quantity or aggregated quantity over a time period.",
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
            "amount",
            "unit_sales"
        ]
    },
    {
        "name": "event_timestamp",
        "description": "Timestamp when the sales event occurred (transaction time or aggregated date).",
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
            "salesdate",
            "date"
        ]
    },
    {
        "name": "unit_price",
        "description": "Price per unit of the product. May be missing in aggregated datasets.",
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
            "cost"
        ]
    },
    {
        "name": "total_value",
        "description": "Total sales value for the record (quantity * unit_price).",
        "expected_dtype": "numeric",
        "required": False,
        "derivable": True,
        "derivation_logic": "quantity * unit_price"
    },
    {
        "name": "customer_id",
        "description": "Unique identifier representing the customer.",
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
        "description": "Geographic region where the sale occurred.",
        "expected_dtype": "string",
        "required": False,
        "possible_source_names": [
            "country",
            "region",
            "state",
            "zone",
            "market",
            "city"
        ]
    },
    {
        "name": "promotion_flag",
        "description": "Indicates whether the item was under promotion during the sale.",
        "expected_dtype": "boolean",
        "required": False,
        "possible_source_names": [
            "onpromotion",
            "promo",
            "is_promo",
            "promotion"
        ]
    }
]