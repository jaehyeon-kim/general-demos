{%- set source_model = 'v_stg_orders' -%}
{%- set src_pk = 'ORDER_PK' -%}
{%- set src_hashdiff = 'ORDER_HASHDIFF' -%}
{%- set src_payload = ["ORDER_STATUS", "TOTAL_PRICE", "ORDER_PRIORITY",
                      "CLERK", "SHIP_PRIORITY", "COMMENT"] -%}
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