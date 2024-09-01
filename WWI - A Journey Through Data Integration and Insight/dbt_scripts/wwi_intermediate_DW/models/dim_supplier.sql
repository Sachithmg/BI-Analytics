{{ config(materialized='table', alias='Supplier') }}

WITH supplier_data AS (
    SELECT
        s.[SupplierID] AS wwi_supplier_id,
        s.[SupplierName] AS supplier,
        s.[SupplierCategoryID] AS category_id,  -- Assuming a placeholder since actual category details are missing
        s.[PrimaryContactPersonID] AS primary_contact_id,  -- Assuming a placeholder since actual contact details are missing
        s.[SupplierReference] AS supplier_reference,
        s.[PaymentDays] AS payment_days,
        city.[CityName] AS city,
        sp.[StateProvinceName] AS province,
        co.[CountryName] AS country,
        s.PostalPostalCode AS postal_code,  -- Assuming address details are available
        s.[ValidFrom] AS valid_from,
        s.[ValidTo] AS valid_to,
        '{{ this.schema }}.SupplierKey' AS lineage_key
    FROM {{ source('intermediate', 'Suppliers') }} s
    LEFT JOIN {{ source('intermediate', 'Cities') }} city ON s.DeliveryCityID = city.CityID
    LEFT JOIN {{ source('intermediate', 'StateProvinces') }} sp ON city.StateProvinceID = sp.StateProvinceID
    LEFT JOIN {{ source('intermediate', 'Countries') }} co ON sp.CountryID = co.CountryID
)

SELECT
    ROW_NUMBER() OVER (ORDER BY wwi_supplier_id) AS supplier_key,
    wwi_supplier_id,
    supplier,
    category_id AS category,  -- Placeholder
    primary_contact_id AS primary_contact,  -- Placeholder
    supplier_reference,
    payment_days,
    city,
    province,
    country,
    postal_code,
    valid_from,
    valid_to,
    lineage_key
FROM supplier_data
