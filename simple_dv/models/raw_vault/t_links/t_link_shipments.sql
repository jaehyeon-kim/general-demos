{%- set source_model = 'v_stg_shipments' -%}
{%- set src_pk = 'SHIPMENT_PK' -%}
{%- set src_fk = ["CUSTOMER_PK", "ORDER_PK"] -%}
{%- set src_payload = ["SHIPMENT_NUMBER", "SHIPMENT_DATE", "AMOUNT"] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = 'LOAD_DATE' -%}
{%- set src_source = 'RECORD_SOURCE' -%}

{{automate_dv.t_link(
  src_pk=src_pk,
  src_fk=src_fk,
  src_ldts=src_ldts,
  src_payload=src_payload,
  src_eff=src_eff,
  src_source=src_source,
  source_model=source_model
)}}