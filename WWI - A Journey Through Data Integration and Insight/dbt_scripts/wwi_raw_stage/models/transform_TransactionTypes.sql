{{ config(materialized='table', alias='TransactionTypes') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_TransactionTypes') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.TransactionTypeID') AS INT) AS TransactionTypeID,
    CAST(JSON_VALUE(json_data, '$.TransactionTypeName') AS NVARCHAR(50)) AS TransactionTypeName,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
