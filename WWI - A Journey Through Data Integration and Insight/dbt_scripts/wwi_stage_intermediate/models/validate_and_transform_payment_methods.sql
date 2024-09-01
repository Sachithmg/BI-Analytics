{{ config(materialized='table', alias='PaymentMethods') }}

WITH source_data AS (
    SELECT
        [PaymentMethodID],
        TRIM([PaymentMethodName]) AS PaymentMethodName,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Check for null values and ensure PaymentMethodID is an integer
        CASE WHEN PaymentMethodID IS NOT NULL AND PaymentMethodName IS NOT NULL AND TRY_CAST(PaymentMethodID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid,
        -- Temporal validity check
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates,
        -- Check for name length constraints
        CASE WHEN LEN(PaymentMethodName) <= 50 THEN 1 ELSE 0 END AS is_valid_length
    FROM [stage].[PaymentMethods]
),
validated_data AS (
    SELECT
        PaymentMethodID,
        PaymentMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid = 1 
          AND is_valid_dates = 1
          AND is_valid_length = 1
),
deduped_data AS (
    -- Handle potential duplicates by consolidating entries with the same method name
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY PaymentMethodName ORDER BY PaymentMethodID) AS rn
    FROM validated_data
)
SELECT
    PaymentMethodID,
    PaymentMethodName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn = 1;  -- Ensures unique PaymentMethodNames are retained
