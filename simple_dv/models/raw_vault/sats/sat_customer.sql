{%- set source_model = 'v_stg_orders' -%}
{%- set src_pk = 'CUSTOMER_PK' -%}
{%- set src_hashdiff = 'CUSTOMER_HASHDIFF' -%}
{%- set src_payload = ["CUSTOMER_NAME", "CUSTOMER_ADDRESS", "CUSTOMER_NATION_KEY",
                      "CUSTOMER_PHONE", "CUSTOMER_ACCT_BAL", "CUSTOMER_MKT_SEGMENT", "CUSTOMER_COMMENT"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{ automate_dv.sat(
  src_pk=src_pk,
  src_hashdiff=src_hashdiff,
  src_payload=src_payload,
  src_eff=src_eff,
  src_ldts=src_ldts,
  src_source=src_source,
  source_model=source_model
)}}