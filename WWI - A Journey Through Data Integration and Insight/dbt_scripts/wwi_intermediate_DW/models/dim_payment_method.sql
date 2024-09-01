{{ config(materialized='table', alias='Payment Method') }}

WITH source_data AS (
    SELECT
        pm.PaymentMethodID AS WWI_Payment_Method_ID,
        pm.PaymentMethodName AS Payment_Method,
        pm.ValidFrom,
        pm.ValidTo,
        pm.LastEditedBy AS Lineage_Key
    FROM [intermediate].[PaymentMethods] pm
)

SELECT
    ROW_NUMBER() OVER (ORDER BY WWI_Payment_Method_ID) AS Payment_Method_Key,  -- Incremental keys, assuming no natural key fits better
    WWI_Payment_Method_ID,
    Payment_Method,
    ValidFrom,
    ValidTo,
    Lineage_Key
FROM source_data;
