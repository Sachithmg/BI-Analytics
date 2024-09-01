{{ config(materialized='table', alias='Stock Holding') }}

WITH StockItemHoldings AS (
    SELECT
        sih.StockItemID AS stock_item_id,
        sih.QuantityOnHand AS quantity_on_hand,
        sih.BinLocation AS bin_location,
        sih.LastStocktakeQuantity AS last_stocktake_quantity,
        sih.LastCostPrice AS last_cost_price,
        sih.ReorderLevel AS reorder_level,
        sih.TargetStockLevel AS target_stock_level,
        sih.LastEditedBy AS lineage_key
    FROM {{ source('intermediate', 'StockItemHoldings') }} AS sih
),

DimensionIntegration AS (
    SELECT
        sih.stock_item_id,
        sih.quantity_on_hand,
        sih.bin_location,
        sih.last_stocktake_quantity,
        sih.last_cost_price,
        sih.reorder_level,
        sih.target_stock_level,
        sih.lineage_key,
        si.stock_item_key
    FROM StockItemHoldings AS sih
    LEFT JOIN {{ source('Dimension', 'Stock Item') }} AS si ON si.wwi_stock_item_id = sih.stock_item_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY stock_item_key) AS stock_holding_key,
    stock_item_key,
    quantity_on_hand,
    bin_location,
    last_stocktake_quantity,
    last_cost_price,
    reorder_level,
    target_stock_level,
    lineage_key
FROM DimensionIntegration
