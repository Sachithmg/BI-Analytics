{{ config(materialized='table',  alias='Customer') }}

WITH customer_data AS (
    SELECT
        c.[CustomerID] AS wwi_customer_id,
        c.[CustomerName] AS customer,
        c.BillToCustomerID AS bill_to_customer_id, -- placeholder for actual name
        c.CustomerCategoryID AS category_id, -- placeholder for actual category
        c.BuyingGroupID AS buying_group_id, -- placeholder for actual group
        c.PrimaryContactPersonID AS primary_contact_id, -- placeholder for actual contact
        city.[CityName] AS city,
        sp.[StateProvinceName] AS province,
        co.[CountryName] AS country,
        c.PostalPostalCode  AS postal_code, -- No addresses table available
        c.[ValidFrom] AS valid_from,
        c.[ValidTo] AS valid_to,
        '{{ this.schema }}.CustomerKey' AS lineage_key
    FROM {{ source('intermediate', 'Customers') }} c
    LEFT JOIN {{ source('intermediate', 'Cities') }} city ON c.DeliveryCityID = city.CityID
    LEFT JOIN {{ source('intermediate', 'StateProvinces') }} sp ON city.StateProvinceID = sp.StateProvinceID
    LEFT JOIN {{ source('intermediate', 'Countries') }} co ON sp.CountryID = co.CountryID
)

SELECT
    ROW_NUMBER() OVER (ORDER BY wwi_customer_id) AS customer_key,
    wwi_customer_id,
    customer,
    bill_to_customer_id AS bill_to_customer, -- Set to ID as placeholder
    category_id AS category, -- Set to ID as placeholder
    buying_group_id AS buying_group, -- Set to ID as placeholder
    primary_contact_id AS primary_contact, -- Set to ID as placeholder
    city,
    province,
    country,
    postal_code,
    valid_from,
    valid_to,
    lineage_key
FROM customer_data
