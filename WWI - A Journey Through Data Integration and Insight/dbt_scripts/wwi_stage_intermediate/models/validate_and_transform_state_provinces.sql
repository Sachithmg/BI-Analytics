{{ config(materialized='table', alias='StateProvinces') }}

WITH source_data AS (
    SELECT
        [StateProvinceID],
        [StateProvinceCode],
        TRIM([StateProvinceName]) AS StateProvinceName,
        [CountryID],
        TRIM([SalesTerritory]) AS SalesTerritory,
        [Border],
        [LatestRecordedPopulation],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN StateProvinceID IS NOT NULL AND StateProvinceName IS NOT NULL AND CountryID IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(StateProvinceID AS INT) IS NOT NULL 
             AND TRY_CAST(CountryID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_types,
        -- Geography Data Integrity
        CASE WHEN Border IS NULL OR Border.STIsValid() = 1 THEN 1 ELSE 0 END AS is_valid_geography,
        -- Range and Value Checks for Population
        CASE WHEN LatestRecordedPopulation IS NULL OR LatestRecordedPopulation >= 0 THEN 1 ELSE 0 END AS is_valid_population,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo AND (ValidFrom <= GETDATE() OR ValidTo IS NULL) THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[StateProvinces]
),
validated_data AS (
    SELECT
        StateProvinceID,
        StateProvinceCode,
        StateProvinceName,
        CountryID,
        SalesTerritory,
        Border,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_types = 1 
          AND is_valid_geography = 1 
          AND is_valid_population = 1 
          AND is_valid_dates = 1
),
deduped_data AS (
    -- Check for duplicates
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CountryID, StateProvinceName ORDER BY StateProvinceID) AS row_number
    FROM validated_data
)
SELECT
    StateProvinceID,
    StateProvinceCode,
    StateProvinceName,
    CountryID,
    SalesTerritory,
    Border,
    LatestRecordedPopulation,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE row_number = 1;
