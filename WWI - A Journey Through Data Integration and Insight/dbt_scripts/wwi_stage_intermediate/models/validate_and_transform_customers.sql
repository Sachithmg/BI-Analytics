{{ config(materialized='table', alias='Customers') }}

WITH source_data AS (
    SELECT
        [CustomerID],
        TRIM([CustomerName]) AS CustomerName,
        [BillToCustomerID],
        [CustomerCategoryID],
        [BuyingGroupID],
        [PrimaryContactPersonID],
        [AlternateContactPersonID],
        [DeliveryMethodID],
        [DeliveryCityID],
        [PostalCityID],
        COALESCE([CreditLimit], 0) AS CreditLimit,  -- Impute null CreditLimit with 0
        [AccountOpenedDate],
        [StandardDiscountPercentage],
        [IsStatementSent],
        [IsOnCreditHold],
        [PaymentDays],
        TRIM([PhoneNumber]) AS PhoneNumber,
        TRIM([FaxNumber]) AS FaxNumber,
        TRIM([DeliveryRun]) AS DeliveryRun,
        TRIM([RunPosition]) AS RunPosition,
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
        -- Check for nulls and primary key uniqueness
        CASE WHEN CustomerID IS NOT NULL AND CustomerName IS NOT NULL THEN 1 ELSE 0 END AS is_valid_primary,
        -- Data type and range checks
        CASE WHEN TRY_CAST(CustomerID AS INT) IS NOT NULL 
             AND TRY_CAST(COALESCE([CreditLimit], 0) AS DECIMAL(18, 2)) IS NOT NULL 
             AND (COALESCE([CreditLimit], 0) >= 0 AND StandardDiscountPercentage >= 0 AND StandardDiscountPercentage <= 100 AND PaymentDays >= 0) THEN 1 ELSE 0 END AS is_valid_types,
        -- Temporal validity
        CASE WHEN ValidFrom <= ValidTo AND AccountOpenedDate <= GETDATE() THEN 1 ELSE 0 END AS is_valid_dates
    FROM [stage].[Customers]
),
validated_data AS (
    SELECT
        CustomerID,
        CustomerName,
        BillToCustomerID,
        CustomerCategoryID,
        BuyingGroupID,
        PrimaryContactPersonID,
        AlternateContactPersonID,
        DeliveryMethodID,
        DeliveryCityID,
        PostalCityID,
        CreditLimit,
        AccountOpenedDate,
        StandardDiscountPercentage,
        IsStatementSent,
        IsOnCreditHold,
        PaymentDays,
        PhoneNumber,
        FaxNumber,
        DeliveryRun,
        RunPosition,
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
    -- Handle potential duplicates by consolidating entries with the same CustomerID
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY ValidFrom DESC) AS rn
    FROM validated_data
)
SELECT
    CustomerID,
    CustomerName,
    BillToCustomerID,
    CustomerCategoryID,
    BuyingGroupID,
    PrimaryContactPersonID,
    AlternateContactPersonID,
    DeliveryMethodID,
    DeliveryCityID,
    PostalCityID,
    CreditLimit,
    AccountOpenedDate,
    StandardDiscountPercentage,
    IsStatementSent,
    IsOnCreditHold,
    PaymentDays,
    PhoneNumber,
    FaxNumber,
    DeliveryRun,
    RunPosition,
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
WHERE rn = 1;  -- Ensures unique CustomerIDs in the final dataset
