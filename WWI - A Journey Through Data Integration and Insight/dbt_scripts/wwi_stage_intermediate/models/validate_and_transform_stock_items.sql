{{ config(materialized='table', alias='StockItems') }}

WITH source_data AS (
    SELECT
        [StockItemID],
        TRIM([StockItemName]) AS StockItemName,
        [SupplierID],
        [ColorID],
        [UnitPackageID],
        [OuterPackageID],
        TRIM([Brand]) AS Brand,
        TRIM([Size]) AS Size,
        [LeadTimeDays],
        [QuantityPerOuter],
        [IsChillerStock],
        TRIM([Barcode]) AS Barcode,
        [TaxRate],
        [UnitPrice],
        [RecommendedRetailPrice],
        [TypicalWeightPerUnit],
        TRIM([MarketingComments]) AS MarketingComments,
        TRIM([InternalComments]) AS InternalComments,
        [Photo],
        TRIM([CustomFields]) AS CustomFields,
        TRIM([Tags]) AS Tags,
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        TRIM([SearchDetails]) AS SearchDetails,
        -- Check for nulls and primary key uniqueness
        CASE WHEN StockItemID IS NOT NULL AND StockItemName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data type and range checks
        CASE WHEN TRY_CAST(StockItemID AS INT) IS NOT NULL 
             AND TRY_CAST(UnitPrice AS DECIMAL(18, 2)) IS NOT NULL 
             AND TRY_CAST(TaxRate AS DECIMAL(18, 3)) IS NOT NULL 
             AND (UnitPrice >= 0 AND TaxRate >= 0 AND LeadTimeDays >= 0) THEN 1 ELSE 0 END AS is_valid_types,
        -- Temporal validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates,
        -- Referential integrity and foreign key checks (mocked as always valid for example)
        1 AS is_valid_fk
    FROM [stage].[StockItems]
),
validated_data AS (
    SELECT
        StockItemID,
        StockItemName,
        SupplierID,
        ColorID,
        UnitPackageID,
        OuterPackageID,
        Brand,
        Size,
        LeadTimeDays,
        QuantityPerOuter,
        IsChillerStock,
        Barcode,
        TaxRate,
        UnitPrice,
        RecommendedRetailPrice,
        TypicalWeightPerUnit,
        MarketingComments,
        InternalComments,
        Photo,
        CustomFields,
        Tags,
        LastEditedBy,
        ValidFrom,
        ValidTo,
        SearchDetails
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_types = 1
          AND is_valid_dates = 1
          AND is_valid_fk = 1
),
deduped_data AS (
    -- Handle duplicate StockItemIDs if needed, ensuring each ID is unique
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY StockItemID ORDER BY ValidFrom DESC) AS rn
    FROM validated_data
)
SELECT
    StockItemID,
    StockItemName,
    SupplierID,
    ColorID,
    UnitPackageID,
    OuterPackageID,
    Brand,
    Size,
    LeadTimeDays,
    QuantityPerOuter,
    IsChillerStock,
    Barcode,
    TaxRate,
    UnitPrice,
    RecommendedRetailPrice,
    TypicalWeightPerUnit,
    MarketingComments,
    InternalComments,
    Photo,
    CustomFields,
    Tags,
    LastEditedBy,
    ValidFrom,
    ValidTo,
    SearchDetails
FROM deduped_data
WHERE rn = 1;  -- Ensures unique StockItemIDs in the final dataset
