{{ config(materialized='table', alias='Colors') }}

WITH source_data AS (
    SELECT
        [ColorID],
        TRIM([ColorName]) AS ColorName,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN ColorID IS NOT NULL AND ColorName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(ColorID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_type,
        -- Text and String Formatting
        CASE WHEN LEN(ColorName) <= 4000 THEN 1 ELSE 0 END AS is_valid_length,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[Colors]
),
validated_data AS (
    SELECT
        ColorID,
        UPPER(TRIM(ColorName)) AS ColorName, -- Standardizing and Trimming ColorName
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
    -- Prevent duplicate ColorNames and ensure only one entry per ColorID
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ColorName ORDER BY ColorID) AS rn_name,
        ROW_NUMBER() OVER (PARTITION BY ColorID ORDER BY ColorID) AS rn_id
    FROM validated_data
)
SELECT
    ColorID,
    ColorName,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn_name = 1 AND rn_id = 1;
