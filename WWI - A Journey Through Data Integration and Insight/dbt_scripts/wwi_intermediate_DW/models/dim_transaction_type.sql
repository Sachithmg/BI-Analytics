{{ config(materialized='table', alias='Transaction Type') }}


WITH source_data AS (
    SELECT
        [TransactionTypeID] AS wwi_transaction_type_id,
        [TransactionTypeName] AS transaction_type,
        [ValidFrom],
        [ValidTo],
        [LastEditedBy] AS lineage_key  -- Assuming using LastEditedBy as a lineage key
    FROM {{ source('intermediate', 'TransactionTypes') }}
)

, keyed_data AS (
    SELECT
        -- Generating a unique key for each transaction type; adjust according to your key management strategy
        ROW_NUMBER() OVER (ORDER BY wwi_transaction_type_id)  AS transaction_type_key,
        wwi_transaction_type_id,
        transaction_type,
        [ValidFrom],
        [ValidTo],
        lineage_key
    FROM source_data
)

SELECT
    transaction_type_key,
    wwi_transaction_type_id,
    transaction_type,
    [ValidFrom],
    [ValidTo],
    lineage_key
FROM keyed_data



