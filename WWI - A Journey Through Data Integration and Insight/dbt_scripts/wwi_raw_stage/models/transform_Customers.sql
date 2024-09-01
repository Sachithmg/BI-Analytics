{{ config(materialized='table', alias='Customers') }}

WITH source_data AS (
    SELECT
        CAST(_airbyte_data AS NVARCHAR(MAX)) AS json_data
    FROM {{ source('raw', 'raw_raw__stream_Customers') }}
)

SELECT
    CAST(JSON_VALUE(json_data, '$.CustomerID') AS INT) AS CustomerID,
    CAST(JSON_VALUE(json_data, '$.CustomerName') AS NVARCHAR(100)) AS CustomerName,
    CAST(JSON_VALUE(json_data, '$.BillToCustomerID') AS INT) AS BillToCustomerID,
    CAST(JSON_VALUE(json_data, '$.CustomerCategoryID') AS INT) AS CustomerCategoryID,
    CAST(JSON_VALUE(json_data, '$.BuyingGroupID') AS INT) AS BuyingGroupID,
    CAST(JSON_VALUE(json_data, '$.PrimaryContactPersonID') AS INT) AS PrimaryContactPersonID,
    CAST(JSON_VALUE(json_data, '$.AlternateContactPersonID') AS INT) AS AlternateContactPersonID,
    CAST(JSON_VALUE(json_data, '$.DeliveryMethodID') AS INT) AS DeliveryMethodID,
    CAST(JSON_VALUE(json_data, '$.DeliveryCityID') AS INT) AS DeliveryCityID,
    CAST(JSON_VALUE(json_data, '$.PostalCityID') AS INT) AS PostalCityID,
    CAST(JSON_VALUE(json_data, '$.CreditLimit') AS DECIMAL(18, 2)) AS CreditLimit,
    CAST(JSON_VALUE(json_data, '$.AccountOpenedDate') AS DATE) AS AccountOpenedDate,
    CAST(JSON_VALUE(json_data, '$.StandardDiscountPercentage') AS DECIMAL(18, 3)) AS StandardDiscountPercentage,
    CAST(JSON_VALUE(json_data, '$.IsStatementSent') AS BIT) AS IsStatementSent,
    CAST(JSON_VALUE(json_data, '$.IsOnCreditHold') AS BIT) AS IsOnCreditHold,
    CAST(JSON_VALUE(json_data, '$.PaymentDays') AS INT) AS PaymentDays,
    CAST(JSON_VALUE(json_data, '$.PhoneNumber') AS NVARCHAR(20)) AS PhoneNumber,
    CAST(JSON_VALUE(json_data, '$.FaxNumber') AS NVARCHAR(20)) AS FaxNumber,
    CAST(JSON_VALUE(json_data, '$.DeliveryRun') AS NVARCHAR(5)) AS DeliveryRun,
    CAST(JSON_VALUE(json_data, '$.RunPosition') AS NVARCHAR(5)) AS RunPosition,
    CAST(JSON_VALUE(json_data, '$.WebsiteURL') AS NVARCHAR(256)) AS WebsiteURL,
    CAST(JSON_VALUE(json_data, '$.DeliveryAddressLine1') AS NVARCHAR(60)) AS DeliveryAddressLine1,
    CAST(JSON_VALUE(json_data, '$.DeliveryAddressLine2') AS NVARCHAR(60)) AS DeliveryAddressLine2,
    CAST(JSON_VALUE(json_data, '$.DeliveryPostalCode') AS NVARCHAR(10)) AS DeliveryPostalCode,
    geography::STPointFromText(JSON_VALUE(json_data, '$.DeliveryLocation'), 4326) AS DeliveryLocation,
    CAST(JSON_VALUE(json_data, '$.PostalAddressLine1') AS NVARCHAR(60)) AS PostalAddressLine1,
    CAST(JSON_VALUE(json_data, '$.PostalAddressLine2') AS NVARCHAR(60)) AS PostalAddressLine2,
    CAST(JSON_VALUE(json_data, '$.PostalPostalCode') AS NVARCHAR(10)) AS PostalPostalCode,
    CAST(JSON_VALUE(json_data, '$.LastEditedBy') AS INT) AS LastEditedBy,
    CAST(JSON_VALUE(json_data, '$.ValidFrom') AS DATETIME2(7)) AS ValidFrom,
    CAST(JSON_VALUE(json_data, '$.ValidTo') AS DATETIME2(7)) AS ValidTo
FROM source_data;
