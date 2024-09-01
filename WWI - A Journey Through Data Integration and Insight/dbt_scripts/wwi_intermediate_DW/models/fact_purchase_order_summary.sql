{{ config(materialized='table', alias='PurchaseOrderSummary') }}


WITH PurchaseOrderLinesAggregated AS (
    SELECT
        ppl.PurchaseOrderID,
        ppl.StockItemID,
        ppl.PackageTypeID,
        SUM(ppl.ExpectedUnitPricePerOuter) / COUNT(ppl.PurchaseOrderLineID) AS AverageUnitPrice,
        SUM(ppl.OrderedOuters) AS OrderOuters,
        SUM(ppl.ReceivedOuters) AS ReceivedOuters,
        MAX(ppl.LastReceiptDate) AS POActualReceivedDate
    FROM {{ source('intermediate', 'PurchaseOrderLines') }} AS ppl
    GROUP BY ppl.PurchaseOrderID, ppl.StockItemID, ppl.PackageTypeID
),

JoinedData AS (
    SELECT
        po.PurchaseOrderID AS purchase_order_key,
        dsi.stock_item_key,
        dst.supplier_key,
        dpt.PackageTypeName,
        dst.city, 
        dst.province,
        dst.country,
        po.OrderDate AS order_date_key,
        po.ExpectedDeliveryDate AS expected_delivery_date_key,
        pol.POActualReceivedDate AS po_actual_received_date_key,
        dst.payment_days AS payment_term_days,
        pol.OrderOuters,
        pol.ReceivedOuters,
        pol.AverageUnitPrice
    FROM {{ source('intermediate', 'PurchaseOrders') }} AS po
    INNER JOIN PurchaseOrderLinesAggregated AS pol ON po.PurchaseOrderID = pol.PurchaseOrderID
    LEFT JOIN {{ source('Dimension', 'Supplier') }} AS dst ON dst.wwi_supplier_id = po.SupplierID
    LEFT JOIN {{ source('Dimension', 'Stock Item') }} AS dsi ON dsi.wwi_stock_item_id = pol.StockItemID
    LEFT JOIN {{ source('intermediate', 'PackageTypes') }} AS dpt ON dpt.PackageTypeID = pol.PackageTypeID
)

SELECT
    purchase_order_key,
    supplier_key,
    city, 
    province,
    country
    stock_item_key,
    PackageTypeName,
    order_date_key,
    expected_delivery_date_key,
    po_actual_received_date_key,
    payment_term_days,
    OrderOuters,
    ReceivedOuters,
    AverageUnitPrice
FROM JoinedData
--ORDER BY purchase_order_key, stock_item_key, package_type_key
