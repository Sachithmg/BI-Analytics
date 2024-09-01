{{ config(materialized='table', alias='Suppliers') }}

WITH source_data AS (
    SELECT
        _airbyte_raw_id,
        _airbyte_data,
        _airbyte_extracted_at,
        _airbyte_loaded_at,
        _airbyte_meta
    FROM {{ source('raw', 'raw_raw__stream_Suppliers') }}
)

SELECT
    CAST(JSON_VALUE(_airbyte_data, '$.SupplierID') AS INT) AS SupplierID,
    CAST(JSON_VALUE(_airbyte_data, '$.SupplierName') AS NVARCHAR(100)) AS SupplierName,
    CAST(JSON_VALUE(_airbyte_data, '$.SupplierCategoryID') AS INT) AS SupplierCategoryID,
    CAST(JSON_VALUE(_airbyte_data, '$.PrimaryContactPersonID') AS INT) AS PrimaryContactPersonID,
    CAST(JSON_VALUE(_airbyte_data, '$.AlternateContactPersonID') AS INT) AS AlternateContactPersonID,
    CAST(JSON_VALUE(_airbyte_data, '$.DeliveryMethodID') AS INT) AS DeliveryMethodID,
    CAST(JSON_VALUE(_airbyte_data, '$.DeliveryCityID') AS INT) AS DeliveryCityID,
    CAST(JSON_VALUE(_airbyte_data, '$.PostalCityID') AS INT) AS PostalCityID,
    CAST(JSON_VALUE(_airbyte_data, '$.SupplierReference') AS NVARCHAR(20)) AS SupplierReference,
    CAST(JSON_VALUE(_airbyte_data, '$.BankAccountName') AS NVARCHAR(50)) AS BankAccountName,
    CAST(JSON_VALUE(_airbyte_data, '$.BankAccountBranch') AS NVARCHAR(50)) AS BankAccountBranch,
    CAST(JSON_VALUE(_airbyte_data, '$.BankAccountCode') AS NVARCHAR(20)) AS BankAccountCode,
    CAST(JSON_VALUE(_airbyte_data, '$.BankAccountNumber') AS NVARCHAR(20)) AS BankAccountNumber,
    CAST(JSON_VALUE(_airbyte_data, '$.BankInternationalCode') AS NVARCHAR(20)) AS BankInternationalCode,
    CAST(JSON_VALUE(_airbyte_data, '$.PaymentDays') AS INT) AS PaymentDays,
    CAST(JSON_VALUE(_airbyte_data, '$.InternalComments') AS NVARCHAR(MAX)) AS InternalComments,
    CAST(JSON_VALUE(_airbyte_data, '$.PhoneNumber') AS NVARCHAR(20)) AS PhoneNumber,
    CAST(JSON_VALUE(_airbyte_data, '$.FaxNumber') AS NVARCHAR(20)) AS FaxNumber,
    CAST(JSON_VALUE(_airbyte_data, '$.WebsiteURL') AS NVARCHAR(256)) AS WebsiteURL,
    CAST(JSON_VALUE(_airbyte_data, '$.DeliveryAddressLine1') AS NVARCHAR(60)) AS DeliveryAddressLine1,
    CAST(JSON_VALUE(_airbyte_data, '$.DeliveryAddressLine2') AS NVARCHAR(60)) AS DeliveryAddressLine2,
    CAST(JSON_VALUE(_airbyte_data, '$.DeliveryPostalCode') AS NVARCHAR(10)) AS DeliveryPostalCode,
    geography::STPointFromText(JSON_VALUE(_airbyte_data, '$.DeliveryLocation'), 4326) AS DeliveryLocation,
    CAST(JSON_VALUE(_airbyte_data, '$.PostalAddressLine1') AS NVARCHAR(60)) AS PostalAddressLine1,
    CAST(JSON_VALUE(_airbyte_data, '$.PostalAddressLine2') AS NVARCHAR(60)) AS PostalAddressLine2,
    CAST(JSON_VALUE(_airbyte_data, '$.PostalPostalCode') AS NVARCHAR(10)) AS PostalPostalCode,
    CAST(JSON_VALUE(_airbyte_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(_airbyte_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(_airbyte_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
