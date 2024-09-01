{{ config(materialized='table', alias='Colors') }}

WITH source_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_data,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        _airbyte_meta
    FROM {{ source('raw', 'raw_raw__stream_Colors') }} -- Replace with the actual reference to your raw source table in dbt
)

, parsed_data AS (
    SELECT
        CAST(JSON_VALUE(_airbyte_data, '$.ColorID') AS INT) AS ColorID,
        JSON_VALUE(_airbyte_data, '$.ColorName') AS ColorName,
        CAST(JSON_VALUE(_airbyte_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
    FROM source_data
)

SELECT
    ColorID,
    ColorName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM parsed_data
WHERE ColorID IS NOT NULL AND ColorName IS NOT NULL;
