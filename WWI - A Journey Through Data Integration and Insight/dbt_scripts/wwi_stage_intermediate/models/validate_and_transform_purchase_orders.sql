{{ config(materialized='table', alias='PurchaseOrders') }}

WITH validated_data AS (
    SELECT
        po.PurchaseOrderID,
        po.SupplierID,
        po.OrderDate,
        po.DeliveryMethodID,
        po.ContactPersonID,
        po.ExpectedDeliveryDate,
        po.SupplierReference,
        po.IsOrderFinalized,
        po.Comments,
        po.InternalComments,
        po.LastEditedBy,
        po.LastEditedWhen,
        -- Uniqueness and Null Checks
        ROW_NUMBER() OVER (PARTITION BY po.PurchaseOrderID ORDER BY po.LastEditedWhen DESC) AS rn,
        -- Data Type Validation
        CASE WHEN ISNUMERIC(po.PurchaseOrderID) = 1
             AND ISNUMERIC(po.SupplierID) = 1
             AND po.OrderDate IS NOT NULL
             --AND ISDATE(po.OrderDate) = 1
             AND (po.DeliveryMethodID IS NULL OR ISNUMERIC(po.DeliveryMethodID) = 1)
             AND (po.ContactPersonID IS NULL OR ISNUMERIC(po.ContactPersonID) = 1)
             AND po.ExpectedDeliveryDate IS NOT NULL
             --AND ISDATE(po.ExpectedDeliveryDate) = 1
             THEN 1 ELSE 0 END AS is_valid_types,
        -- Referential Integrity Checks
        CASE WHEN EXISTS (SELECT 1 FROM [stage].[Suppliers] WHERE SupplierID = po.SupplierID)
             AND (po.DeliveryMethodID IS NULL OR EXISTS (SELECT 1 FROM [stage].[DeliveryMethods] WHERE DeliveryMethodID = po.DeliveryMethodID))
             --AND (po.ContactPersonID IS NULL OR EXISTS (SELECT 1 FROM [stage].[Contacts] WHERE ContactPersonID = po.ContactPersonID))
             THEN 1 ELSE 0 END AS is_valid_references,
        -- Date Validity Checks
        CASE WHEN po.OrderDate <= po.ExpectedDeliveryDate
             THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[PurchaseOrders] po
),
filtered_data AS (
    SELECT
        PurchaseOrderID,
        SupplierID,
        OrderDate,
        DeliveryMethodID,
        ContactPersonID,
        ExpectedDeliveryDate,
        SupplierReference,
        IsOrderFinalized,
        Comments,
        InternalComments,
        LastEditedBy,
        LastEditedWhen
    FROM validated_data
    WHERE rn = 1 
          AND is_valid_types = 1 
          AND is_valid_references = 1
          AND is_valid_dates = 1
)
SELECT * FROM filtered_data;
