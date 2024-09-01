{{ config(materialized='table', alias='StateProvinces') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_StateProvinces') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.StateProvinceID') AS INT) AS StateProvinceID,
    CAST(JSON_VALUE(json_data, '$.StateProvinceCode') AS NVARCHAR(5)) AS StateProvinceCode,
    CAST(JSON_VALUE(json_data, '$.StateProvinceName') AS NVARCHAR(50)) AS StateProvinceName,
    CAST(JSON_VALUE(json_data, '$.CountryID') AS INT) AS CountryID,
    CAST(JSON_VALUE(json_data, '$.SalesTerritory') AS NVARCHAR(50)) AS SalesTerritory,
    geography::STGeomFromText(CAST(JSON_VALUE(json_data, '$.Border') AS NVARCHAR(MAX)), 4326) AS Border,
    CAST(JSON_VALUE(json_data, '$.LatestRecordedPopulation') AS BIGINT) AS LatestRecordedPopulation,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
