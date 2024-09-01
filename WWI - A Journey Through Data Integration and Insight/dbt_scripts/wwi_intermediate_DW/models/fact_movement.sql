{{ config(materialized='table', alias='Movement') }}


WITH StockItemTransactions AS (
    SELECT
        sit.StockItemTransactionID AS wwi_stock_item_transaction_id,
        sit.StockItemID AS stock_item_id,
        sit.TransactionTypeID AS transaction_type_id,
        sit.CustomerID AS customer_id,
        sit.SupplierID AS supplier_id,
        sit.InvoiceID AS wwi_invoice_id,
        sit.PurchaseOrderID AS wwi_purchase_order_id,
        CAST(sit.TransactionOccurredWhen AS DATE) AS transaction_date,  -- Convert datetime to date
        sit.Quantity,
        sit.LastEditedBy AS lineage_key
    FROM {{  source('intermediate', 'StockItemTransactions') }} AS sit
),

JoinedDimensions AS (
    SELECT
        sit.wwi_stock_item_transaction_id,
        d.Date as date_key,
        si.stock_item_key,
        c.customer_key,
        s.supplier_key,
        tt.transaction_type_key,
        sit.wwi_invoice_id,
        sit.wwi_purchase_order_id,
        sit.Quantity,
        sit.lineage_key
    FROM StockItemTransactions AS sit
    LEFT JOIN {{ source('Dimension', 'Date') }} AS d ON d.Date = sit.transaction_date
    LEFT JOIN {{ source('Dimension', 'Stock Item') }} AS si ON si.wwi_stock_item_id = sit.stock_item_id
    LEFT JOIN {{ source('Dimension', 'Customer') }} AS c ON c.wwi_customer_id = sit.customer_id
    LEFT JOIN {{ source('Dimension', 'Supplier') }} AS s ON s.wwi_supplier_id = sit.supplier_id
    LEFT JOIN {{ source('Dimension', 'Transaction Type') }} AS tt ON tt.wwi_transaction_type_id = sit.transaction_type_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date_key, stock_item_key) AS movement_key,
    date_key,
    stock_item_key,
    customer_key,
    supplier_key,
    transaction_type_key,
    wwi_stock_item_transaction_id AS wwi_stock_item_transaction_id,
    wwi_invoice_id,
    wwi_purchase_order_id,
    Quantity,
    lineage_key
FROM JoinedDimensions

