{{ config(materialized='table', alias='Invoices') }}

WITH source_data AS (
    SELECT
        [InvoiceID],
        [CustomerID],
        [BillToCustomerID],
        [OrderID],
        [DeliveryMethodID],
        [ContactPersonID],
        [AccountsPersonID],
        [SalespersonPersonID],
        [PackedByPersonID],
        [InvoiceDate],
        TRIM([CustomerPurchaseOrderNumber]) AS CustomerPurchaseOrderNumber,
        [IsCreditNote],
        TRIM([CreditNoteReason]) AS CreditNoteReason,
        TRIM([Comments]) AS Comments,
        TRIM([DeliveryInstructions]) AS DeliveryInstructions,
        TRIM([InternalComments]) AS InternalComments,
        [TotalDryItems],
        [TotalChillerItems],
        TRIM([DeliveryRun]) AS DeliveryRun,
        TRIM([RunPosition]) AS RunPosition,
        TRIM([ReturnedDeliveryData]) AS ReturnedDeliveryData,
        COALESCE([ConfirmedDeliveryTime], CAST('2099-12-31' AS DATETIME2)) AS ConfirmedDeliveryTime, -- Assigning a default future date if NULL
        TRIM([ConfirmedReceivedBy]) AS ConfirmedReceivedBy,
        [LastEditedBy],
        [LastEditedWhen],
        -- Primary Key and Null Checks
        CASE WHEN InvoiceID IS NOT NULL 
              AND CustomerID IS NOT NULL 
              AND InvoiceDate IS NOT NULL 
              AND OrderID IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data Type Consistency
        CASE WHEN TRY_CAST(InvoiceID AS INT) IS NOT NULL 
              AND TRY_CAST(CustomerID AS INT) IS NOT NULL THEN 1 ELSE 0 END AS is_valid_type,
        -- Text and String Formatting
        CASE WHEN LEN(CustomerPurchaseOrderNumber) <= 4000 THEN 1 ELSE 0 END AS is_valid_length,
        -- Temporal Validity
        CASE WHEN InvoiceDate <= COALESCE([ConfirmedDeliveryTime], CAST('2099-12-31' AS DATETIME2)) THEN 1 ELSE 0 END AS is_valid_dates, -- Temporal validation with adjusted date
        -- Business Logic Checks
        CASE WHEN IsCreditNote = 1 AND CreditNoteReason IS NOT NULL THEN 1 ELSE 0 END AS is_valid_credit_note
    FROM [stage].[Invoices]
),
validated_data AS (
    SELECT
        InvoiceID,
        CustomerID,
        BillToCustomerID,
        OrderID,
        DeliveryMethodID,
        ContactPersonID,
        AccountsPersonID,
        SalespersonPersonID,
        PackedByPersonID,
        InvoiceDate,
        CustomerPurchaseOrderNumber,
        IsCreditNote,
        CreditNoteReason,
        Comments,
        DeliveryInstructions,
        InternalComments,
        TotalDryItems,
        TotalChillerItems,
        DeliveryRun,
        RunPosition,
        ReturnedDeliveryData,
        ConfirmedDeliveryTime,
        ConfirmedReceivedBy,
        LastEditedBy,
        LastEditedWhen
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_type = 1
          AND is_valid_length = 1 
          AND is_valid_dates = 1
          AND (IsCreditNote = 0 OR is_valid_credit_note = 1)
),
deduped_data AS (
    -- Ensure unique InvoiceID
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY InvoiceID ORDER BY LastEditedWhen DESC) AS rn
    FROM validated_data
)
SELECT
    InvoiceID,
    CustomerID,
    BillToCustomerID,
    OrderID,
    DeliveryMethodID,
    ContactPersonID,
    AccountsPersonID,
    SalespersonPersonID,
    PackedByPersonID,
    InvoiceDate,
    CustomerPurchaseOrderNumber,
    IsCreditNote,
    CreditNoteReason,
    Comments,
    DeliveryInstructions,
    InternalComments,
    TotalDryItems,
    TotalChillerItems,
    DeliveryRun,
    RunPosition,
    ReturnedDeliveryData,
    ConfirmedDeliveryTime,
    ConfirmedReceivedBy,
    LastEditedBy,
    LastEditedWhen
FROM deduped_data
WHERE rn = 1;

