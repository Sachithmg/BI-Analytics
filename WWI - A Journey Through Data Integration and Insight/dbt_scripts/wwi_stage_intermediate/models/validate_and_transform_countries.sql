{{ config(materialized='table', alias='Countries') }}

WITH source_data AS (
    SELECT
        [CountryID],
        TRIM([CountryName]) AS CountryName,
        TRIM([FormalName]) AS FormalName,
        TRIM([IsoAlpha3Code]) AS IsoAlpha3Code,
        [IsoNumericCode],
        TRIM([CountryType]) AS CountryType,
        [LatestRecordedPopulation],
        TRIM([Continent]) AS Continent,
        TRIM([Region]) AS Region,
        TRIM([Subregion]) AS Subregion,
        [Border],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN CountryID IS NOT NULL AND CountryName IS NOT NULL AND IsoAlpha3Code IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency and ISO Code Validation
        CASE WHEN TRY_CAST(CountryID AS INT) IS NOT NULL 
             AND LEN(IsoAlpha3Code) = 3 
             AND (IsoNumericCode IS NULL OR (IsoNumericCode BETWEEN 1 AND 999)) THEN 1 ELSE 0 END AS is_valid_codes,
        -- Range and Value Checks for Population
        CASE WHEN LatestRecordedPopulation IS NULL OR LatestRecordedPopulation >= 0 THEN 1 ELSE 0 END AS is_valid_population,
        -- Temporal Validity
        CASE WHEN ValidFrom <= ValidTo AND (ValidFrom <= GETDATE() OR ValidTo IS NULL) THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[Countries]
),
validated_data AS (
    SELECT
        CountryID,
        CountryName,
        FormalName,
        IsoAlpha3Code,
        IsoNumericCode,
        CountryType,
        LatestRecordedPopulation,
        Continent,
        Region,
        Subregion,
        Border,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_codes = 1 
          AND is_valid_population = 1 
          AND is_valid_dates = 1
),
deduped_data AS (
    -- Check for duplicates in CountryName and FormalName
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CountryName ORDER BY CountryID) AS rn_country,
        ROW_NUMBER() OVER (PARTITION BY FormalName ORDER BY CountryID) AS rn_formal
    FROM validated_data
)
SELECT
    CountryID,
    CountryName,
    FormalName,
    IsoAlpha3Code,
    IsoNumericCode,
    CountryType,
    LatestRecordedPopulation,
    Continent,
    Region,
    Subregion,
    Border,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn_country = 1 AND rn_formal = 1;
