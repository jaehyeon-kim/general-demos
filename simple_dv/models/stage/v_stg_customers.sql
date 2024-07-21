{%- set yaml_metadata -%}
source_model: 'raw_customers'
derived_columns:
  RECORD_SOURCE: '!TPCH-CUSTOMER'
hashed_columns:
  CUSTOMER_PK: 'CUSTOMER_KEY'
  LINK_CUSTOMER_ORDER_PK