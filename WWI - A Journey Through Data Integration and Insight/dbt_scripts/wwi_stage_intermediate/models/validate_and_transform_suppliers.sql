{{ config(materialized='table', alias='Suppliers') }}

WITH source_data AS (
    SELECT
        [SupplierID],
        TRIM([SupplierName]) AS SupplierName,
        [SupplierCategoryID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        TRIM([SupplierReference]) AS SupplierReference,
        TRIM([BankAccountName]) AS BankAccountName,
        TRIM([BankAccountBranch]) AS BankAccountBranch,
        TRIM([BankAccountCode]) AS BankAccountCode,
        TRIM([BankAccountNumber]) AS BankAccountNumber,
        TRIM([BankInternationalCode]) AS BankInternationalCode,
        [PaymentDays],
        TRIM([InternalComments]) AS InternalComments,
        TRIM([PhoneNumber]) AS PhoneNumber,
        TRIM([FaxNumber]) AS FaxNumber,
        TRIM([WebsiteURL]) AS WebsiteURL,
        TRIM([DeliveryAddressLine1]) AS DeliveryAddressLine1,
        TRIM([DeliveryAddressLine2]) AS DeliveryAddressLine2,
        [DeliveryPostalCode],
        [DeliveryLocation],
        TRIM([PostalAddressLine1]) AS PostalAddressLine1,
        TRIM([PostalAddressLine2]) AS PostalAddressLine2,
        [PostalPostalCode],
        [LastEditedBy],
        [ValidFrom],
        [ValidTo],
        -- Primary Key and Null Checks
        CASE WHEN SupplierID IS NOT NULL AND SupplierName IS NOT NULL AND SupplierCategoryID IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data type and range checks
        CASE WHEN TRY_CAST(SupplierID AS INT) IS NOT NULL AND TRY_CAST(PaymentDays AS INT) IS NOT NULL AND PaymentDays >= 0 THEN 1 ELSE 0 END AS is_valid_types,
        -- Temporal validity
        CASE WHEN ValidFrom <= ValidTo THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[Suppliers]
),
validated_data AS (
    SELECT
        SupplierID,
        SupplierName,
        SupplierCategoryID,
        PrimaryContactPersonID,
        AlternateContactPersonID,
        DeliveryMethodID,
        DeliveryCityID,
        PostalCityID,
        SupplierReference,
        BankAccountName,
        BankAccountBranch,
        BankAccountCode,
        BankAccountNumber,
        BankInternationalCode,
        PaymentDays,
        InternalComments,
        PhoneNumber,
        FaxNumber,
        WebsiteURL,
        DeliveryAddressLine1,
        DeliveryAddressLine2,
        DeliveryPostalCode,
        DeliveryLocation,
        PostalAddressLine1,
        PostalAddressLine2,
        PostalPostalCode,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM source_data
    WHERE is_valid_primary = 1 
          AND is_valid_types = 1 
          AND is_valid_dates = 1
),
deduped_data AS (
    -- Handle potential duplicates by consolidating entries with the same SupplierID
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SupplierID ORDER BY ValidFrom DESC) AS rn
    FROM validated_data
)
SELECT
    SupplierID,
    SupplierName,
    SupplierCategoryID,
    PrimaryContactPersonID,
    AlternateContactPersonID,
    DeliveryMethodID,
    DeliveryCityID,
    PostalCityID,
    SupplierReference,
    BankAccountName,
    BankAccountBranch,
    BankAccountCode,
    BankAccountNumber,
    BankInternationalCode,
    PaymentDays,
    InternalComments,
    PhoneNumber,
    FaxNumber,
    WebsiteURL,
    DeliveryAddressLine1,
    DeliveryAddressLine2,
    DeliveryPostalCode,
    DeliveryLocation,
    PostalAddressLine1,
    PostalAddressLine2,
    PostalPostalCode,
    LastEditedBy,
    ValidFrom,
    ValidTo
FROM deduped_data
WHERE rn = 1;  -- Ensures unique SupplierIDs in the final dataset
