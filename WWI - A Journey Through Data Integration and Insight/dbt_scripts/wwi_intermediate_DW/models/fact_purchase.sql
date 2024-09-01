{{ config(materialized='table', alias='Purchase') }}


WITH PurchaseOrders AS (
    SELECT
        po.PurchaseOrderID AS wwi_purchase_order_id,
        po.SupplierID AS supplier_id,
        po.OrderDate,
        po.ExpectedDeliveryDate,
        po.IsOrderFinalized,
        po.LastEditedBy AS lineage_key
    FROM {{ source('intermediate', 'PurchaseOrders')  }} AS po
),

PurchaseOrderLines AS (
    SELECT
        pol.PurchaseOrderID,
        pol.StockItemID AS stock_item_id,
        pol.OrderedOuters,
        pol.Description,
        pol.ReceivedOuters,
        pol.PackageTypeID,
        pol.ExpectedUnitPricePerOuter,
        pol.LastEditedBy AS pol_lineage_key,
        pt.PackageTypeName AS package
    FROM {{ source('intermediate', 'PurchaseOrderLines')  }} AS pol
    LEFT JOIN {{ source('intermediate', 'PackageTypes') }} AS pt ON pol.PackageTypeID = pt.PackageTypeID
),

JoinedOrders AS (
    SELECT
        po.wwi_purchase_order_id,
        po.supplier_id,
        po.OrderDate,
        po.IsOrderFinalized,
        po.lineage_key AS po_lineage_key,
        pol.stock_item_id,
        pol.OrderedOuters,
        pol.ReceivedOuters,
        pol.package,
        pol.ExpectedUnitPricePerOuter,
        pol.pol_lineage_key,
        (pol.OrderedOuters * pol.ExpectedUnitPricePerOuter) AS ordered_quantity  -- Assuming 'expected_units_per_outer' is available from context
    FROM PurchaseOrders AS po
    JOIN PurchaseOrderLines AS pol ON po.wwi_purchase_order_id = pol.PurchaseOrderID
),

DimensionIntegration AS (
    SELECT
        jo.wwi_purchase_order_id,
        d.Date AS order_date_key,
        si.stock_item_key,
        s.supplier_key,
        jo.OrderedOuters,
        jo.ReceivedOuters,
        jo.ordered_quantity,
        jo.package,
        jo.IsOrderFinalized,
        jo.po_lineage_key AS lineage_key
    FROM JoinedOrders AS jo
    LEFT JOIN {{ source('Dimension', 'Date')}} AS d ON d.Date = jo.OrderDate
    LEFT JOIN {{ source('Dimension', 'Stock Item') }} AS si ON si.wwi_stock_item_id = jo.stock_item_id
    LEFT JOIN {{ source('Dimension', 'Supplier') }} AS s ON s.wwi_supplier_id = jo.supplier_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY order_date_key, stock_item_key, supplier_key) AS purchase_key,
    order_date_key AS date_key,
    supplier_key,
    stock_item_key,
    wwi_purchase_order_id,
    OrderedOuters,
    ordered_quantity,
    ReceivedOuters,
    package,
    IsOrderFinalized,
    lineage_key
FROM DimensionIntegration
