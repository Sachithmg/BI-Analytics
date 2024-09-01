{{ config(materialized='table', alias='PackageTypes') }}

WITH source_data AS (
    SELECT
        [PackageTypeID],
        TRIM([PackageTypeName]) AS PackageTypeName,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN PackageTypeID IS NOT NULL AND PackageTypeName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(PackageTypeID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_type,
        -- Text and String Formatting
        CASE WHEN LEN(PackageTypeName) <= 4000 THEN 1 ELSE 0 END AS is_valid_length,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[PackageTypes]
),
validated_data AS (
    SELECT
        PackageTypeID,
        UPPER(TRIM(PackageTypeName)) AS PackageTypeName, -- Standardizing and Trimming PackageTypeName
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
    -- Ensure unique PackageTypeID and PackageTypeName
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY PackageTypeID ORDER BY PackageTypeID) AS rn_id,
        ROW_NUMBER() OVER (PARTITION BY PackageTypeName ORDER BY PackageTypeID) AS rn_name
    FROM validated_data
)
SELECT
    PackageTypeID,
    PackageTypeName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn_id = 1 AND rn_name = 1;
