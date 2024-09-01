{{ config(materialized='table', alias='Cities') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_Cities') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.CityID') AS INT) AS CityID,
    CAST(JSON_VALUE(json_data, '$.CityName') AS NVARCHAR(50)) AS CityName,
    CAST(JSON_VALUE(json_data, '$.StateProvinceID') AS INT) AS StateProvinceID,
    geography::STPointFromText(JSON_VALUE(json_data, '$.Location'), 4326) AS Location,
    CAST(JSON_VALUE(json_data, '$.LatestRecordedPopulation') AS BIGINT) AS LatestRecordedPopulation,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
