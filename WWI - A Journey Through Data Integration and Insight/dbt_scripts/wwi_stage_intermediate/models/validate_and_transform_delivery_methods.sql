{{ config(materialized='table', alias='DeliveryMethods') }}

WITH source_data AS (
    SELECT
        [DeliveryMethodID],
        TRIM([DeliveryMethodName]) AS DeliveryMethodName,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN DeliveryMethodID IS NOT NULL AND DeliveryMethodName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(DeliveryMethodID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_type,
        -- Text and String Formatting
        CASE WHEN LEN(DeliveryMethodName) <= 4000 THEN 1 ELSE 0 END AS is_valid_length,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[DeliveryMethods]
),
validated_data AS (
    SELECT
        DeliveryMethodID,
        UPPER(TRIM(DeliveryMethodName)) AS DeliveryMethodName, -- Standardizing and Trimming DeliveryMethodName
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
    -- Ensure unique DeliveryMethodID and DeliveryMethodName
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY DeliveryMethodID ORDER BY DeliveryMethodID) AS rn_id,
        ROW_NUMBER() OVER (PARTITION BY DeliveryMethodName ORDER BY DeliveryMethodID) AS rn_name
    FROM validated_data
)
SELECT
    DeliveryMethodID,
    DeliveryMethodName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn_id = 1 AND rn_name = 1;
