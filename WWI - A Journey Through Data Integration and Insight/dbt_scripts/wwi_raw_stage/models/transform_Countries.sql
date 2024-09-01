{{ config(materialized='table', alias='Countries') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_Countries') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.CountryID') AS INT) AS CountryID,
    CAST(JSON_VALUE(json_data, '$.CountryName') AS NVARCHAR(60)) AS CountryName,
    CAST(JSON_VALUE(json_data, '$.FormalName') AS NVARCHAR(60)) AS FormalName,
    CAST(JSON_VALUE(json_data, '$.IsoAlpha3Code') AS NVARCHAR(3)) AS IsoAlpha3Code,
    CAST(JSON_VALUE(json_data, '$.IsoNumericCode') AS INT) AS IsoNumericCode,
    CAST(JSON_VALUE(json_data, '$.CountryType') AS NVARCHAR(20)) AS CountryType,
    CAST(JSON_VALUE(json_data, '$.LatestRecordedPopulation') AS BIGINT) AS LatestRecordedPopulation,
    CAST(JSON_VALUE(json_data, '$.Continent') AS NVARCHAR(30)) AS Continent,
    CAST(JSON_VALUE(json_data, '$.Region') AS NVARCHAR(30)) AS Region,
    CAST(JSON_VALUE(json_data, '$.Subregion') AS NVARCHAR(30)) AS Subregion,
    geography::STGeomFromText(CAST(JSON_VALUE(json_data, '$.Border') AS NVARCHAR(MAX)), 4326) AS Border,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
