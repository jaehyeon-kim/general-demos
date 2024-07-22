{%- set yaml_metadata -%}
source_model: 'raw_shipments'
derived_columns:
  RECORD_SOURCE: '!RAW_SHIPMENTS'
  LOAD_DATE: DATEADD(DAY, 1, SHIPMENT_DATE)
  EFFECTIVE_FROM: 'SHIPMENT_DATE'
hashed_columns:
  SHIPMENT_PK:
    - 'CUSTOMER_KEY'
    - 'ORDER_KEY'
  CUSTOMER_PK: 'CUSTOMER_KEY'
  ORDER_PK: 'ORDER_KEY'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=none) }}