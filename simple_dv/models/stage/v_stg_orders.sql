{%- set yaml_metadata -%}
source_model: 'raw_orders'
derived_columns:
  RECORD_SOURCE: '!TPCH-ORDER'
  EFFECTIVE_FROM: 'ORDER_DATE'
hashed_columns:
  CUSTOMER_PK: 'CUSTOMER_KEY'
  ORDER_PK: 'ORDER_KEY'
  ORDER_CUSTOMER_PK:
    - 'CUSTOMER_KEY'
    - 'ORDER_KEY'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'CUSTOMER_KEY'
      - 'CUSTOMER_NAME'
      - 'CUSTOMER_ADDRESS'
      - 'CUSTOMER_NATION_KEY'
      - 'CUSTOMER_PHONE'
      - 'CUSTOMER_ACCT_BAL'
      - 'CUSTOMER_MKT_SEGMENT'
      - 'CUSTOMER_COMMENT'
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ORDER_KEY'
      - 'ORDER_STATUS'
      - 'TOTAL_PRICE'
      - 'ORDER_PRIORITY'
      - 'CLERK'
      - 'SHIP_PRIORITY'
      - 'COMMENT'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
  {{ automate_dv.stage(
    include_source_columns=true,
    source_model=source_model,
    derived_columns=derived_columns,
    hashed_columns=hashed_columns,
    ranked_columns=none
  ) }}
)
SELECT
  *
  , TO_DATE('{{ var('load_date')}}') AS LOAD_DATE
FROM staging
