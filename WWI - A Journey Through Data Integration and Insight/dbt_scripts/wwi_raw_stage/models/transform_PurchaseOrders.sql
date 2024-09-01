{{ config(materialized='table', alias='PurchaseOrders') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_PurchaseOrders') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.PurchaseOrderID') AS INT) AS PurchaseOrderID,
    CAST(JSON_VALUE(json_data, '$.SupplierID') AS INT) AS SupplierID,
    CAST(JSON_VALUE(json_data, '$.OrderDate') AS DATE) AS OrderDate,
    CAST(JSON_VALUE(json_data, '$.DeliveryMethodID') AS INT) AS DeliveryMethodID,
    CAST(JSON_VALUE(json_data, '$.ContactPersonID') AS INT) AS ContactPersonID,
    CAST(JSON_VALUE(json_data, '$.ExpectedDeliveryDate') AS DATE) AS ExpectedDeliveryDate,
    CAST(JSON_VALUE(json_data, '$.SupplierReference') AS NVARCHAR(20)) AS SupplierReference,
    CAST(JSON_VALUE(json_data, '$.IsOrderFinalized') AS BIT) AS IsOrderFinalized,
    CAST(JSON_VALUE(json_data, '$.Comments') AS NVARCHAR(MAX)) AS Comments,
    CAST(JSON_VALUE(json_data, '$.InternalComments') AS NVARCHAR(MAX)) AS InternalComments,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
