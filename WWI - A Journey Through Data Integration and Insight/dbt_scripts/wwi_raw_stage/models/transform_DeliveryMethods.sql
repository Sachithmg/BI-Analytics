{{ config(materialized='table', alias='DeliveryMethods') }}

WITH source_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_data,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        _airbyte_meta
    FROM {{ source('raw', 'raw_raw__stream_DeliveryMethods') }}  -- Ensure this reference matches your source table name in dbt
)

, parsed_data AS (
    SELECT
        CAST(JSON_VALUE(_airbyte_data, '$.DeliveryMethodID') AS INT) AS DeliveryMethodID,
        JSON_VALUE(_airbyte_data, '$.DeliveryMethodName') AS DeliveryMethodName,
        CAST(JSON_VALUE(_airbyte_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
        CAST(JSON_VALUE(_airbyte_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
    FROM source_data
)

SELECT
    DeliveryMethodID,
    DeliveryMethodName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM parsed_data
WHERE DeliveryMethodID IS NOT NULL;
