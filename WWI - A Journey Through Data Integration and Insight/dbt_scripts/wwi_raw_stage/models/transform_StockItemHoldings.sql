{{ config(materialized='table', alias='StockItemHoldings') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_StockItemHoldings') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.StockItemID') AS INT) AS StockItemID,
    CAST(JSON_VALUE(json_data, '$.QuantityOnHand') AS INT) AS QuantityOnHand,
    CAST(JSON_VALUE(json_data, '$.BinLocation') AS NVARCHAR(20)) AS BinLocation,
    CAST(JSON_VALUE(json_data, '$.LastStocktakeQuantity') AS INT) AS LastStocktakeQuantity,
    CAST(JSON_VALUE(json_data, '$.LastCostPrice') AS DECIMAL(18, 2)) AS LastCostPrice,
    CAST(JSON_VALUE(json_data, '$.ReorderLevel') AS INT) AS ReorderLevel,
    CAST(JSON_VALUE(json_data, '$.TargetStockLevel') AS INT) AS TargetStockLevel,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.LastEditedWhen') AS DATETIME2(7)) AS LastEditedWhen
FROM source_data;
