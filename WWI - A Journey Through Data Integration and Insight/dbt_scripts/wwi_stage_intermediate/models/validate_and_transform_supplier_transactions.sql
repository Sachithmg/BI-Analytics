{{ config(materialized='table', alias='SupplierTransactions') }}

WITH validated_data AS (
    SELECT
        st.SupplierTransactionID,
        st.SupplierID,
        st.TransactionTypeID,
        st.PurchaseOrderID,
        st.PaymentMethodID,
        st.SupplierInvoiceNumber,
        st.TransactionDate,
        st.AmountExcludingTax,
        st.TaxAmount,
        st.TransactionAmount,
        st.OutstandingBalance,
        st.FinalizationDate,
        st.IsFinalized,
        st.LastEditedBy,
        st.LastEditedWhen,
        -- Uniqueness and Null Checks
        ROW_NUMBER() OVER (PARTITION BY st.SupplierTransactionID ORDER BY st.LastEditedWhen DESC) AS rn,
        -- Data Type Validation
        CASE WHEN ISNUMERIC(st.SupplierTransactionID) = 1
            AND ISNUMERIC(st.SupplierID) = 1
            AND ISNUMERIC(st.TransactionTypeID) = 1
            AND (st.PurchaseOrderID IS NULL OR ISNUMERIC(st.PurchaseOrderID) = 1)
            AND (st.PaymentMethodID IS NULL OR ISNUMERIC(st.PaymentMethodID) = 1)
            AND st.TransactionDate IS NOT NULL
            AND st.FinalizationDate IS NOT NULL
            AND ISNUMERIC(st.AmountExcludingTax) = 1
            AND ISNUMERIC(st.TaxAmount) = 1
            AND ISNUMERIC(st.TransactionAmount) = 1
            AND ISNUMERIC(st.OutstandingBalance) = 1
            THEN 1 ELSE 0 END AS is_valid_types,
        -- Referential Integrity Checks
        CASE WHEN EXISTS (SELECT 1 FROM [stage].[Suppliers] WHERE SupplierID = st.SupplierID)
             AND EXISTS (SELECT 1 FROM [stage].[TransactionTypes] WHERE TransactionTypeID = st.TransactionTypeID)
             AND (st.PurchaseOrderID IS NULL OR EXISTS (SELECT 1 FROM [stage].[PurchaseOrders] WHERE PurchaseOrderID = st.PurchaseOrderID))
             AND (st.PaymentMethodID IS NULL OR EXISTS (SELECT 1 FROM [stage].[PaymentMethods] WHERE PaymentMethodID = st.PaymentMethodID))
             THEN 1 ELSE 0 END AS is_valid_references,
        -- Financial and Logical Checks
        CASE WHEN st.AmountExcludingTax + st.TaxAmount = st.TransactionAmount
             AND st.OutstandingBalance >= 0
             AND (st.IsFinalized = 1 AND st.FinalizationDate IS NOT NULL AND st.OutstandingBalance = 0)
             THEN 1 ELSE 0 END AS is_valid_financial_logic
    FROM [stage].[SupplierTransactions] st
),
filtered_data AS (
    SELECT
        SupplierTransactionID,
        SupplierID,
        TransactionTypeID,
        PurchaseOrderID,
        PaymentMethodID,
        SupplierInvoiceNumber,
        TransactionDate,
        AmountExcludingTax,
        TaxAmount,
        TransactionAmount,
        OutstandingBalance,
        FinalizationDate,
        IsFinalized,
        LastEditedBy,
        LastEditedWhen
    FROM validated_data
    WHERE rn = 1 
          AND is_valid_types = 1 
          AND is_valid_references = 1
          AND is_valid_financial_logic = 1
)
SELECT * FROM filtered_data;
