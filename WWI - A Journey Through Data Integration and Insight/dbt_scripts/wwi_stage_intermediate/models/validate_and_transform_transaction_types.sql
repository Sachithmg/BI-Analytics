{{ config(materialized='table', alias='TransactionTypes') }}

WITH source_data AS (
    SELECT
        [TransactionTypeID],
        TRIM([TransactionTypeName]) AS TransactionTypeName,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN TransactionTypeID IS NOT NULL AND TransactionTypeName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(TransactionTypeID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_type,
        -- Text and String Formatting
        CASE WHEN LEN(TransactionTypeName) <= 50 THEN 1 ELSE 0 END AS is_valid_length,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[TransactionTypes]
),
validated_data AS (
    SELECT
        TransactionTypeID,
        TransactionTypeName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_type = 1
          AND is_valid_length = 1 
          AND is_valid_dates = 1
),
deduped_data AS (
    -- Prevent duplicate TransactionTypeNames
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY TransactionTypeName ORDER BY TransactionTypeID) AS rn
    FROM validated_data
)
SELECT
    TransactionTypeID,
    TransactionTypeName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn = 1;
