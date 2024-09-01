{{ config(materialized='table', alias='PaymentMethods') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_PaymentMethods') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.PaymentMethodID') AS INT) AS PaymentMethodID,
    CAST(JSON_VALUE(json_data, '$.PaymentMethodName') AS NVARCHAR(50)) AS PaymentMethodName,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
