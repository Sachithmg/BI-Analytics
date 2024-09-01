{{ config(materialized='table', alias='StockItemHoldings') }}

WITH source_data AS (
    SELECT
        [StockItemID],
        [QuantityOnHand],
        TRIM([BinLocation]) AS BinLocation,
        [LastStocktakeQuantity],
        [LastCostPrice],
        [ReorderLevel],
        [TargetStockLevel],
        [LastEditedBy],
        [LastEditedWhen],
        -- Null and Primary Key Checks
        CASE WHEN StockItemID IS NOT NULL AND QuantityOnHand IS NOT NULL AND LastCostPrice IS NOT NULL THEN 1 ELSE 0 END AS is_valid_keys,
        -- Data type and range checks
        CASE WHEN TRY_CAST(StockItemID AS INT) IS NOT NULL
             AND TRY_CAST(QuantityOnHand AS INT) IS NOT NULL AND QuantityOnHand >= 0
             AND TRY_CAST(LastCostPrice AS DECIMAL(18, 2)) IS NOT NULL AND LastCostPrice >= 0 THEN 1 ELSE 0 END AS is_valid_types,
        -- Logical consistency
        CASE WHEN ReorderLevel <= TargetStockLevel THEN 1 ELSE 0 END AS is_valid_logic,
        -- Temporal validity
        CASE WHEN LastEditedWhen <= GETDATE() THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[StockItemHoldings]
),
validated_data AS (
    SELECT
        StockItemID,
        QuantityOnHand,
        BinLocation,
        LastStocktakeQuantity,
        LastCostPrice,
        ReorderLevel,
        TargetStockLevel,
        LastEditedBy,
        LastEditedWhen
    FROM source_data
    WHERE is_valid_keys = 1 
          AND is_valid_types = 1 
          AND is_valid_logic = 1
          AND is_valid_dates = 1
),
deduped_data AS (
    -- Handle potential duplicates by consolidating entries with the same StockItemID and BinLocation
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY StockItemID, BinLocation ORDER BY LastEditedWhen DESC) AS rn
    FROM validated_data
)
SELECT
    StockItemID,
    QuantityOnHand,
    BinLocation,
    LastStocktakeQuantity,
    LastCostPrice,
    ReorderLevel,
    TargetStockLevel,
    LastEditedBy,
    LastEditedWhen
FROM deduped_data
WHERE rn = 1;  -- Ensures unique records for each StockItemID and BinLocation in the final dataset
