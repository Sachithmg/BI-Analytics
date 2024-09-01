{{ config(materialized='table', alias='CustomerTransactions') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_CustomerTransactions') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.CustomerTransactionID') AS INT) AS CustomerTransactionID,
    CAST(JSON_VALUE(json_data, '$.CustomerID') AS INT) AS CustomerID,
    CAST(JSON_VALUE(json_data, '$.TransactionTypeID') AS INT) AS TransactionTypeID,
    CAST(JSON_VALUE(json_data, '$.InvoiceID') AS INT) AS InvoiceID,
    CAST(JSON_VALUE(json_data, '$.PaymentMethodID') AS INT) AS PaymentMethodID,
    CAST(JSON_VALUE(json_data, '$.TransactionDate') AS DATE) AS TransactionDate,
    CAST(JSON_VALUE(json_data, '$.AmountExcludingTax') AS DECIMAL(18, 2)) AS AmountExcludingTax,
    CAST(JSON_VALUE(json_data, '$.TaxAmount') AS DECIMAL(18, 2)) AS TaxAmount,
    CAST(JSON_VALUE(json_data, '$.TransactionAmount') AS DECIMAL(18, 2)) AS TransactionAmount,
    CAST(JSON_VALUE(json_data, '$.OutstandingBalance') AS DECIMAL(18, 2)) AS OutstandingBalance,
    CAST(JSON_VALUE(json_data, '$.FinalizationDate') AS DATE) AS FinalizationDate,
    CASE
        WHEN JSON_VALUE(json_data, '$.FinalizationDate') IS NOT NULL THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS IsFinalized,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
