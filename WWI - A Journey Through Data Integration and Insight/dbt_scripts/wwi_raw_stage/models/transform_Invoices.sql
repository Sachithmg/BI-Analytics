{{ config(materialized='table', alias='Invoices') }}

WITH source_data AS (
    SELECT
        _airbyte_data,
        _airbyte_extracted_at,
        _airbyte_loaded_at
    FROM {{ source('raw', 'raw_raw__stream_Invoices') }}  -- Replace with your actual source table reference
)

, parsed_data AS (
    SELECT
        CAST(JSON_VALUE(_airbyte_data, '$.InvoiceID') AS INT) AS InvoiceID,
        CAST(JSON_VALUE(_airbyte_data, '$.CustomerID') AS INT) AS CustomerID,
        CAST(JSON_VALUE(_airbyte_data, '$.BillToCustomerID') AS INT) AS BillToCustomerID,
        CAST(JSON_VALUE(_airbyte_data, '$.OrderID') AS INT) AS OrderID,
        CAST(JSON_VALUE(_airbyte_data, '$.DeliveryMethodID') AS INT) AS DeliveryMethodID,
        CAST(JSON_VALUE(_airbyte_data, '$.ContactPersonID') AS INT) AS ContactPersonID,
        CAST(JSON_VALUE(_airbyte_data, '$.AccountsPersonID') AS INT) AS AccountsPersonID,
        CAST(JSON_VALUE(_airbyte_data, '$.SalespersonPersonID') AS INT) AS SalespersonPersonID,
        CAST(JSON_VALUE(_airbyte_data, '$.PackedByPersonID') AS INT) AS PackedByPersonID,
        CAST(JSON_VALUE(_airbyte_data, '$.InvoiceDate') AS DATE) AS InvoiceDate,
        JSON_VALUE(_airbyte_data, '$.CustomerPurchaseOrderNumber') AS CustomerPurchaseOrderNumber,
        CAST(JSON_VALUE(_airbyte_data, '$.IsCreditNote') AS BIT) AS IsCreditNote,
        JSON_VALUE(_airbyte_data, '$.CreditNoteReason') AS CreditNoteReason,
        JSON_VALUE(_airbyte_data, '$.Comments') AS Comments,
        JSON_VALUE(_airbyte_data, '$.DeliveryInstructions') AS DeliveryInstructions,
        JSON_VALUE(_airbyte_data, '$.InternalComments') AS InternalComments,
        CAST(JSON_VALUE(_airbyte_data, '$.TotalDryItems') AS INT) AS TotalDryItems,
        CAST(JSON_VALUE(_airbyte_data, '$.TotalChillerItems') AS INT) AS TotalChillerItems,
        JSON_VALUE(_airbyte_data, '$.DeliveryRun') AS DeliveryRun,
        JSON_VALUE(_airbyte_data, '$.RunPosition') AS RunPosition,
        JSON_VALUE(_airbyte_data, '$.ReturnedDeliveryData') AS ReturnedDeliveryData,
        JSON_VALUE(_airbyte_data, '$.ConfirmedReceivedBy') AS ConfirmedReceivedBy,
        CAST(JSON_VALUE(_airbyte_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
        CAST(JSON_VALUE(_airbyte_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
    FROM source_data
)

SELECT
    InvoiceID,
    CustomerID,
    BillToCustomerID,
    OrderID,
    DeliveryMethodID,
    ContactPersonID,
    AccountsPersonID,
    SalespersonPersonID,
    PackedByPersonID,
    InvoiceDate,
    CustomerPurchaseOrderNumber,
    IsCreditNote,
    CreditNoteReason,
    Comments,
    DeliveryInstructions,
    InternalComments,
    TotalDryItems,
    TotalChillerItems,
    DeliveryRun,
    RunPosition,
    ReturnedDeliveryData,
    TRY_CONVERT(datetime2(7), JSON_VALUE(ReturnedDeliveryData, '$.DeliveredWhen'), 126) AS ConfirmedDeliveryTime,
    ConfirmedReceivedBy,
    LastEditedBy,
    LastEditedWhen
FROM parsed_data
WHERE InvoiceID IS NOT NULL;
