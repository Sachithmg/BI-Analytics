{{ config(materialized='table', alias='Cities') }}

WITH source_data AS (
    SELECT
        CityID,
        CityName,
        StateProvinceID,
        Location,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo,
        -- Validate CityID and CityName for nulls and ensure CityID is an integer
        CASE 
            WHEN CityID IS NOT NULL AND CityName IS NOT NULL AND TRY_CAST(CityID AS INT) IS NOT NULL THEN 1 
            ELSE 0 
        END AS is_valid,
        -- Validate Location for proper geography format if provided
        CASE 
            WHEN Location IS NULL OR Location.STIsValid() = 1 THEN 1 
            ELSE 0 
        END AS is_valid_location,
        -- Validate ValidFrom and ValidTo for logical consistency and against future dates
        CASE 
            WHEN ValidFrom <= ValidTo AND ValidFrom <= GETDATE() THEN 1 
            ELSE 0 
        END AS is_valid_dates
    FROM [stage].[Cities]
),
valid_data AS (
    SELECT
        CityID,
        LTRIM(RTRIM(CityName)) AS CityName,
        StateProvinceID,
        Location,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid = 1 AND is_valid_location = 1 AND is_valid_dates = 1 
),
deduped_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY StateProvinceID, CityName ORDER BY CityID) AS rn
    FROM valid_data
)
SELECT
    CityID,
    CityName,
    StateProvinceID,
    Location,
    LatestRecordedPopulation,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn = 1;  -- This selects the first valid record of duplicates, ensuring unique city names within states
