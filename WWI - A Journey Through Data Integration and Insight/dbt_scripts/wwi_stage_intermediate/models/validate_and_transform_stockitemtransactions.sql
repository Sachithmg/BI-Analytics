{{ config(materialized='table', alias='StockItemTransactions') }}

WITH source_data AS (
    SELECT
        [StockItemTransactionID],
        [StockItemID],
        [TransactionTypeID],
        [CustomerID],
        [InvoiceID],
        [SupplierID],
        [PurchaseOrderID],
        [TransactionOccurredWhen],
        [Quantity],
        [LastEditedBy],
        [LastEditedWhen],
        -- Primary Key and Null Checks
        CASE WHEN StockItemTransactionID IS NOT NULL AND StockItemID IS NOT NULL AND TransactionTypeID IS NOT NULL AND TransactionOccurredWhen IS NOT NULL THEN 1 ELSE 0 END AS is_valid_keys,
        -- Data type and range checks
        CASE WHEN TRY_CAST(StockItemTransactionID AS INT) IS NOT NULL
             AND TRY_CAST(StockItemID AS INT) IS NOT NULL
             AND TRY_CAST(TransactionTypeID AS INT) IS NOT NULL
             AND TRY_CAST(Quantity AS DECIMAL(18, 3)) IS NOT NULL  -- No non-negative check here
             --AND Quantity >= 0  -- Adjust this condition if negatives are allowed in certain contexts
             AND TransactionOccurredWhen <= GETDATE() THEN 1 ELSE 0 END AS is_valid_types,
        -- Referential integrity and logical consistency
        CASE WHEN EXISTS (SELECT 1 FROM [stage].[StockItems] WHERE StockItemID = [stage].[StockItemTransactions].StockItemID)
             AND EXISTS (SELECT 1 FROM [stage].[TransactionTypes] WHERE TransactionTypeID = [stage].[StockItemTransactions].TransactionTypeID)
             AND (CustomerID IS NULL OR EXISTS (SELECT 1 FROM [stage].[Customers] WHERE CustomerID = [stage].[StockItemTransactions].CustomerID))
             AND (SupplierID IS NULL OR EXISTS (SELECT 1 FROM [stage].[Suppliers] WHERE SupplierID = [stage].[StockItemTransactions].SupplierID))
             AND (InvoiceID IS NULL OR EXISTS (SELECT 1 FROM [stage].[Invoices] WHERE InvoiceID = [stage].[StockItemTransactions].InvoiceID))
             AND (PurchaseOrderID IS NULL OR EXISTS (SELECT 1 FROM [stage].[PurchaseOrders] WHERE PurchaseOrderID = [stage].[StockItemTransactions].PurchaseOrderID))
             THEN 1 ELSE 0 END AS is_valid_references
    FROM [stage].[StockItemTransactions]
),
validated_data AS (
    SELECT
        StockItemTransactionID,
        StockItemID,
        TransactionTypeID,
        CustomerID,
        InvoiceID,
        SupplierID,
        PurchaseOrderID,
        TransactionOccurredWhen,
        Quantity,
        LastEditedBy,
        LastEditedWhen
    FROM source_data
    WHERE is_valid_keys = 1 
          AND is_valid_types = 1 
          AND is_valid_references = 1
),
deduped_data AS (
    -- Handle potential duplicates by keeping the latest record based on LastEditedWhen
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY StockItemTransactionID ORDER BY LastEditedWhen DESC) AS rn
    FROM validated_data
)
SELECT
    StockItemTransactionID,
    StockItemID,
    TransactionTypeID,
    CustomerID,
    InvoiceID,
    SupplierID,
    PurchaseOrderID,
    TransactionOccurredWhen,
    Quantity,
    LastEditedBy,
    LastEditedWhen
FROM deduped_data
WHERE rn = 1;  -- Ensures unique records for each StockItemTransactionID in the final dataset
