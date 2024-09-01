{{ config(materialized='table', alias='CustomerTransactions') }}

WITH validated_data AS (
    SELECT
        ct.CustomerTransactionID,
        ct.CustomerID,
        ct.TransactionTypeID,
        ct.InvoiceID,
        ct.PaymentMethodID,
        ct.TransactionDate,
        ct.AmountExcludingTax,
        ct.TaxAmount,
        ct.TransactionAmount,
        ct.OutstandingBalance,
        ct.FinalizationDate,
        ct.IsFinalized,
        ct.LastEditedBy,
        ct.LastEditedWhen,
        -- Uniqueness and Null Checks
        ROW_NUMBER() OVER (PARTITION BY ct.CustomerTransactionID ORDER BY ct.LastEditedWhen DESC) AS rn,
        -- Data Type Validation
        CASE WHEN ISNUMERIC(ct.CustomerTransactionID) = 1
             AND ISNUMERIC(ct.CustomerID) = 1
             AND ISNUMERIC(ct.TransactionTypeID) = 1
             AND (ct.InvoiceID IS NULL OR ISNUMERIC(ct.InvoiceID) = 1)
             AND (ct.PaymentMethodID IS NULL OR ISNUMERIC(ct.PaymentMethodID) = 1)
             AND ct.TransactionDate IS NOT NULL
             --AND ISDATE(ct.TransactionDate) = 1
             AND ISNUMERIC(ct.AmountExcludingTax) = 1
             AND ISNUMERIC(ct.TaxAmount) = 1
             AND ISNUMERIC(ct.TransactionAmount) = 1
             AND ISNUMERIC(ct.OutstandingBalance) = 1
             THEN 1 ELSE 0 END AS is_valid_types,
        -- Referential Integrity Checks
        CASE WHEN EXISTS (SELECT 1 FROM [stage].[Customers] WHERE CustomerID = ct.CustomerID)
             AND EXISTS (SELECT 1 FROM [stage].[TransactionTypes] WHERE TransactionTypeID = ct.TransactionTypeID)
             --AND (ct.InvoiceID IS NULL OR EXISTS (SELECT 1 FROM [stage].[Invoices] WHERE InvoiceID = ct.InvoiceID))
             AND (ct.PaymentMethodID IS NULL OR EXISTS (SELECT 1 FROM [stage].[PaymentMethods] WHERE PaymentMethodID = ct.PaymentMethodID))
             THEN 1 ELSE 0 END AS is_valid_references,
        -- Financial and Logical Checks
        CASE WHEN ct.AmountExcludingTax + ct.TaxAmount = ct.TransactionAmount
             AND ct.OutstandingBalance >= 0
             AND ((ct.IsFinalized = 1 AND ct.FinalizationDate IS NOT NULL AND ct.OutstandingBalance = 0) OR ct.IsFinalized = 0 )
             THEN 1 ELSE 0 END AS is_valid_financial_logic
    FROM [stage].[CustomerTransactions] ct
),
filtered_data AS (
    SELECT
        CustomerTransactionID,
        CustomerID,
        TransactionTypeID,
        InvoiceID,
        PaymentMethodID,
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
