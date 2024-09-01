{{ config(materialized='table', alias='SupplierTransactions') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_SupplierTransactions') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.SupplierTransactionID') AS INT) AS SupplierTransactionID,
    CAST(JSON_VALUE(json_data, '$.SupplierID') AS INT) AS SupplierID,
    CAST(JSON_VALUE(json_data, '$.TransactionTypeID') AS INT) AS TransactionTypeID,
    CAST(JSON_VALUE(json_data, '$.PurchaseOrderID') AS INT) AS PurchaseOrderID,
    CAST(JSON_VALUE(json_data, '$.PaymentMethodID') AS INT) AS PaymentMethodID,
    CAST(JSON_VALUE(json_data, '$.SupplierInvoiceNumber') AS NVARCHAR(20)) AS SupplierInvoiceNumber,
    CAST(JSON_VALUE(json_data, '$.TransactionDate') AS DATE) AS TransactionDate,
    CAST(JSON_VALUE(json_data, '$.AmountExcludingTax') AS DECIMAL(18, 2)) AS AmountExcludingTax,
    CAST(JSON_VALUE(json_data, '$.TaxAmount') AS DECIMAL(18, 2)) AS TaxAmount,
    CAST(JSON_VALUE(json_data, '$.TransactionAmount') AS DECIMAL(18, 2)) AS TransactionAmount,
    CAST(JSON_VALUE(json_data, '$.OutstandingBalance') AS DECIMAL(18, 2)) AS OutstandingBalance,
    CAST(JSON_VALUE(json_data, '$.FinalizationDate') AS DATE) AS FinalizationDate,
    CAST(IIF(JSON_VALUE(json_data, '$.FinalizationDate') IS NOT NULL, 1, 0) AS BIT) AS IsFinalized,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
