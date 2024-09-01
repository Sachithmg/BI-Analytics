{{ config(materialized='table', alias='StockItems') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_StockItems') }}
),
parsed_data AS (
    SELECT
        CAST(JSON_VALUE(json_data, '$.StockItemID') AS INT) AS StockItemID,
        CAST(JSON_VALUE(json_data, '$.StockItemName') AS NVARCHAR(100)) AS StockItemName,
        CAST(JSON_VALUE(json_data, '$.SupplierID') AS INT) AS SupplierID,
        CAST(JSON_VALUE(json_data, '$.ColorID') AS INT) AS ColorID,
        CAST(JSON_VALUE(json_data, '$.UnitPackageID') AS INT) AS UnitPackageID,
        CAST(JSON_VALUE(json_data, '$.OuterPackageID') AS INT) AS OuterPackageID,
        CAST(JSON_VALUE(json_data, '$.Brand') AS NVARCHAR(50)) AS Brand,
        CAST(JSON_VALUE(json_data, '$.Size') AS NVARCHAR(20)) AS Size,
        CAST(JSON_VALUE(json_data, '$.LeadTimeDays') AS INT) AS LeadTimeDays,
        CAST(JSON_VALUE(json_data, '$.QuantityPerOuter') AS INT) AS QuantityPerOuter,
        CAST(JSON_VALUE(json_data, '$.IsChillerStock') AS BIT) AS IsChillerStock,
        CAST(JSON_VALUE(json_data, '$.Barcode') AS NVARCHAR(50)) AS Barcode,
        CAST(JSON_VALUE(json_data, '$.TaxRate') AS DECIMAL(18, 3)) AS TaxRate,
        CAST(JSON_VALUE(json_data, '$.UnitPrice') AS DECIMAL(18, 2)) AS UnitPrice,
        CAST(JSON_VALUE(json_data, '$.RecommendedRetailPrice') AS DECIMAL(18, 2)) AS RecommendedRetailPrice,
        CAST(JSON_VALUE(json_data, '$.TypicalWeightPerUnit') AS DECIMAL(18, 3)) AS TypicalWeightPerUnit,
        CAST(JSON_VALUE(json_data, '$.MarketingComments') AS NVARCHAR(MAX)) AS MarketingComments,
        CAST(JSON_VALUE(json_data, '$.InternalComments') AS NVARCHAR(MAX)) AS InternalComments,
        NULL AS Photo,  -- Assuming photo handling is external
        CAST(JSON_VALUE(json_data, '$.CustomFields') AS NVARCHAR(MAX)) AS CustomFields,
        JSON_QUERY(CAST(JSON_VALUE(json_data, '$.CustomFields') AS NVARCHAR(MAX)), '$.Tags') AS Tags,
        CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
        CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
        CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
    FROM source_data
)
SELECT *,
    (StockItemName + ' ' + ISNULL(MarketingComments, '')) AS SearchDetails
FROM parsed_data;
