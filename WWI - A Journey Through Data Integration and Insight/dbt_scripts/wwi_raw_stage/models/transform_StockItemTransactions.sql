{{ config(materialized='table', alias='StockItemTransactions') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_StockItemTransactions') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.StockItemTransactionID') AS INT) AS StockItemTransactionID,
    CAST(JSON_VALUE(json_data, '$.StockItemID') AS INT) AS StockItemID,
    CAST(JSON_VALUE(json_data, '$.TransactionTypeID') AS INT) AS TransactionTypeID,
    CAST(JSON_VALUE(json_data, '$.CustomerID') AS INT) AS CustomerID,
    CAST(JSON_VALUE(json_data, '$.InvoiceID') AS INT) AS InvoiceID,
    CAST(JSON_VALUE(json_data, '$.SupplierID') AS INT) AS SupplierID,
    CAST(JSON_VALUE(json_data, '$.PurchaseOrderID') AS INT) AS PurchaseOrderID,
    CAST(JSON_VALUE(json_data, '$.TransactionOccurredWhen') AS DATETIME2(7)) AS TransactionOccurredWhen,
    CAST(JSON_VALUE(json_data, '$.Quantity') AS DECIMAL(18, 3)) AS Quantity,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
