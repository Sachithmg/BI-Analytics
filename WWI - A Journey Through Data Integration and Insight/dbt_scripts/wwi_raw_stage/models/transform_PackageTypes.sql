{{ config(materialized='table', alias='PackageTypes') }}

WITH source_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_data,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        _airbyte_meta
    FROM {{ source('raw', 'raw_raw__stream_PackageTypes') }}  -- Adjust the reference according to your source table name in dbt
)

, parsed_data AS (
    SELECT
        CAST(JSON_VALUE(_airbyte_data, '$.PackageTypeID') AS INT) AS PackageTypeID,
        JSON_VALUE(_airbyte_data, '$.PackageTypeName') AS PackageTypeName,
        CAST(JSON_VALUE(_airbyte_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
    FROM source_data
)

SELECT
    PackageTypeID,
    PackageTypeName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM parsed_data
WHERE PackageTypeID IS NOT NULL;
