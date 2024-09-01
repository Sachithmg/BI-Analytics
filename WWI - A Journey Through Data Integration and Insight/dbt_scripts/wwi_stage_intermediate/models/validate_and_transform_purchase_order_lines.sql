{{ config(materialized='table',  alias='PurchaseOrderLines') }}

WITH validated_data AS (
    SELECT
        pol.PurchaseOrderLineID,
        pol.PurchaseOrderID,
        pol.StockItemID,
        pol.OrderedOuters,
        pol.Description,
        pol.ReceivedOuters,
        pol.PackageTypeID,
        pol.ExpectedUnitPricePerOuter,
        pol.LastReceiptDate,
        pol.IsOrderLineFinalized,
        pol.LastEditedBy,
        pol.LastEditedWhen,
        -- Uniqueness and Null Checks
        ROW_NUMBER() OVER (PARTITION BY pol.PurchaseOrderLineID ORDER BY pol.LastEditedWhen DESC) AS rn,
        -- Data Type Validation
        CASE WHEN ISNUMERIC(pol.PurchaseOrderLineID) = 1
             AND ISNUMERIC(pol.PurchaseOrderID) = 1
             AND ISNUMERIC(pol.StockItemID) = 1
             AND ISNUMERIC(pol.OrderedOuters) = 1
             AND ISNUMERIC(pol.ReceivedOuters) = 1
             AND (pol.PackageTypeID IS NULL OR ISNUMERIC(pol.PackageTypeID) = 1)
             AND pol.LastReceiptDate IS NOT NULL
             --AND ISDATE(pol.LastReceiptDate) = 1
             THEN 1 ELSE 0 END AS is_valid_types,
        -- Referential Integrity Checks
        CASE WHEN EXISTS (SELECT 1 FROM [stage].[PurchaseOrders] WHERE PurchaseOrderID = pol.PurchaseOrderID)
             AND EXISTS (SELECT 1 FROM [stage].[StockItems] WHERE StockItemID = pol.StockItemID)
             AND (pol.PackageTypeID IS NULL OR EXISTS (SELECT 1 FROM [stage].[PackageTypes] WHERE PackageTypeID = pol.PackageTypeID))
             THEN 1 ELSE 0 END AS is_valid_references,
        -- Date Validity Checks
        CASE WHEN pol.LastReceiptDate >= (SELECT OrderDate FROM [stage].[PurchaseOrders] WHERE PurchaseOrderID = pol.PurchaseOrderID)
             THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[PurchaseOrderLines] pol
),
filtered_data AS (
    SELECT
        PurchaseOrderLineID,
        PurchaseOrderID,
        StockItemID,
        OrderedOuters,
        Description,
        ReceivedOuters,
        PackageTypeID,
        ExpectedUnitPricePerOuter,
        LastReceiptDate,
        IsOrderLineFinalized,
        LastEditedBy,
        LastEditedWhen
    FROM validated_data
    WHERE rn = 1 
          AND is_valid_types = 1 
          AND is_valid_references = 1
          AND is_valid_dates = 1
)
SELECT * FROM filtered_data;
