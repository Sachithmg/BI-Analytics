{{ config(materialized='table', alias='Transaction') }}

WITH CustomerTransactions AS (
    SELECT
        ct.CustomerTransactionID AS wwi_customer_transaction_id,
        ct.TransactionDate AS transaction_date,
        ct.CustomerID AS customer_id,
        ct.TransactionTypeID AS transaction_type_id,
        ct.PaymentMethodID AS payment_method_id,
        ct.InvoiceID AS wwi_invoice_id,
        ct.AmountExcludingTax AS total_excluding_tax,
        ct.TaxAmount AS tax_amount,
        ct.TransactionAmount AS total_including_tax,
        ct.OutstandingBalance,
        ct.IsFinalized,
        ct.LastEditedBy AS lineage_key
    FROM {{ source('intermediate', 'CustomerTransactions') }} AS ct
),

SupplierTransactions AS (
    SELECT
        st.SupplierTransactionID AS wwi_supplier_transaction_id,
        st.TransactionDate AS transaction_date,
        st.SupplierID AS supplier_id,
        st.PurchaseOrderID AS wwi_purchase_order_id,
        st.SupplierInvoiceNumber,
        st.PaymentMethodID AS payment_method_id,
        st.AmountExcludingTax AS total_excluding_tax,
        st.TaxAmount AS tax_amount,
        st.TransactionAmount AS total_including_tax,
        st.OutstandingBalance,
        st.IsFinalized,
        st.LastEditedBy AS lineage_key
    FROM {{ source('intermediate', 'SupplierTransactions') }} AS st
),

JoinedTransactions AS (
    SELECT
        ct.wwi_customer_transaction_id,
        st.wwi_supplier_transaction_id,
        ct.transaction_date,
        ct.customer_id,
        st.supplier_id,
        ct.transaction_type_id,
        ct.payment_method_id,
        ct.wwi_invoice_id,
        st.wwi_purchase_order_id,
        st.SupplierInvoiceNumber,
        ct.total_excluding_tax,
        ct.tax_amount,
        ct.total_including_tax,
        ct.OutstandingBalance,
        ct.IsFinalized,
        ct.lineage_key
    FROM CustomerTransactions AS ct
    FULL OUTER JOIN SupplierTransactions AS st
        ON ct.transaction_date = st.transaction_date
),

TransactionDetails AS (
    SELECT
        jt.*,
        dm.Date,
        c.customer_key,
        s.supplier_key,
        tt.transaction_type_key,
        pm.payment_method_key
    FROM JoinedTransactions AS jt
    LEFT JOIN {{ source('Dimension', 'Date') }} AS dm ON dm.Date = jt.transaction_date
    LEFT JOIN {{ source('Dimension', 'Customer') }} AS c ON jt.customer_id = c.wwi_customer_id
    LEFT JOIN {{ source('Dimension', 'Supplier') }} AS s ON jt.supplier_id = s.wwi_supplier_id
    LEFT JOIN {{ source('Dimension', 'Transaction Type') }} AS tt ON jt.transaction_type_id = tt.wwi_transaction_type_id
    LEFT JOIN {{ source('Dimension', 'Payment Method') }} AS pm ON jt.payment_method_id = pm.wwi_payment_method_id
)

SELECT
    ROW_NUMBER() OVER (ORDER BY date) AS transaction_key,
    jt.Date,
    jt.customer_key,
    jt.supplier_key,
    jt.transaction_type_key,
    jt.payment_method_key,
    jt.wwi_customer_transaction_id,
    jt.wwi_supplier_transaction_id,
    jt.wwi_invoice_id,
    jt.wwi_purchase_order_id,
    jt.SupplierInvoiceNumber,
    jt.total_excluding_tax,
    jt.tax_amount,
    jt.total_including_tax,
    jt.OutstandingBalance,
    jt.IsFinalized,
    jt.lineage_key
FROM TransactionDetails AS jt

