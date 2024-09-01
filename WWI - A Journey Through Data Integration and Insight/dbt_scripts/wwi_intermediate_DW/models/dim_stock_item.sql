{{ config(materialized='table', alias='Stock Item') }}

WITH stock_items AS (
    SELECT
        si.[StockItemID] AS wwi_stock_item_id,
        si.[StockItemName] AS stock_item,
        c.[ColorName] AS color,
        pt.[PackageTypeName] AS selling_package,
        pb.[PackageTypeName] AS buying_package,
        si.[Brand] AS brand,
        si.[Size] AS size,
        si.[LeadTimeDays] AS lead_time_days,
        si.[QuantityPerOuter] AS quantity_per_outer,
        si.[IsChillerStock] AS is_chiller_stock,
        si.[Barcode] AS barcode,
        si.[TaxRate] AS tax_rate,
        si.[UnitPrice] AS unit_price,
        si.[RecommendedRetailPrice] AS recommended_retail_price,
        si.[TypicalWeightPerUnit] AS typical_weight_per_unit,
        si.[Photo] AS photo,
        si.[ValidFrom] AS valid_from,
        si.[ValidTo] AS valid_to,
        '{{ this.schema }}.StockItemKey' AS lineage_key
    FROM {{ source('intermediate', 'StockItems') }} si
    LEFT JOIN {{ source('intermediate', 'Colors') }} c ON si.ColorID = c.ColorID
    LEFT JOIN {{ source('intermediate', 'PackageTypes') }} pt ON si.OuterPackageID = pt.PackageTypeID
    LEFT JOIN {{ source('intermediate', 'PackageTypes') }} pb ON si.UnitPackageID = pb.PackageTypeID
)

SELECT
    ROW_NUMBER() OVER (ORDER BY wwi_stock_item_id) AS stock_item_key,
    wwi_stock_item_id,
    stock_item,
    color,
    selling_package,
    buying_package,
    brand,
    size,
    lead_time_days,
    quantity_per_outer,
    is_chiller_stock,
    barcode,
    tax_rate,
    unit_price,
    recommended_retail_price,
    typical_weight_per_unit,
    photo,
    valid_from,
    valid_to,
    lineage_key
FROM stock_items
