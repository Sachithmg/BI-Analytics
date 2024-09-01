{{ config(materialized='table', alias='PurchaseOrderLines') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_PurchaseOrderLines') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.PurchaseOrderLineID') AS INT) AS PurchaseOrderLineID,
    CAST(JSON_VALUE(json_data, '$.PurchaseOrderID') AS INT) AS PurchaseOrderID,
    CAST(JSON_VALUE(json_data, '$.StockItemID') AS INT) AS StockItemID,
    CAST(JSON_VALUE(json_data, '$.OrderedOuters') AS INT) AS OrderedOuters,
    CAST(JSON_VALUE(json_data, '$.Description') AS NVARCHAR(100)) AS Description,
    CAST(JSON_VALUE(json_data, '$.ReceivedOuters') AS INT) AS ReceivedOuters,
    CAST(JSON_VALUE(json_data, '$.PackageTypeID') AS INT) AS PackageTypeID,
    CAST(JSON_VALUE(json_data, '$.ExpectedUnitPricePerOuter') AS DECIMAL(18, 2)) AS ExpectedUnitPricePerOuter,
    CAST(JSON_VALUE(json_data, '$.LastReceiptDate') AS DATE) AS LastReceiptDate,
    CAST(JSON_VALUE(json_data, '$.IsOrderLineFinalized') AS BIT) AS IsOrderLineFinalized,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
