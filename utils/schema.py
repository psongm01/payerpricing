"""PyArrow schemas shared by the TiC ingestion scripts."""

import pyarrow as pa


LINEAGE_FIELDS = [
    pa.field("payer_code", pa.string()),
    pa.field("file_month", pa.string()),
    pa.field("state", pa.string()),
    pa.field("source_version", pa.string()),
    pa.field("etl_run_id", pa.string()),
    pa.field("ingested_at", pa.string()),
]


PLAN_PRICING_BRIDGE_SCHEMA = pa.schema(
    [
        pa.field("reporting_entity_name", pa.string()),
        pa.field("reporting_entity_type", pa.string()),
        pa.field("last_updated_on", pa.string()),
        pa.field("plan_name", pa.string()),
        pa.field("plan_id", pa.string()),
        pa.field("plan_id_type", pa.string()),
        pa.field("plan_market_type", pa.string()),
        pa.field("plan_sponser_name", pa.string()),
        pa.field("issuer_name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("location", pa.string()),
        pa.field("source_index_file", pa.string()),
        *LINEAGE_FIELDS,
    ]
)


TIC_PROVIDER_REFERENCE_SCHEMA = pa.schema(
    [
        pa.field("source_pricing_file", pa.string()),
        pa.field("provider_group_id", pa.int64()),
        pa.field("network_name", pa.string()),
        pa.field("tin_type", pa.string()),
        pa.field("tin_value", pa.string()),
        pa.field("npi", pa.string()),
        pa.field("business_name", pa.string()),
        *LINEAGE_FIELDS,
    ]
)


TIC_PRICE_SCHEMA = pa.schema(
    [
        pa.field("source_pricing_file", pa.string()),
        pa.field("reporting_entity_name", pa.string()),
        pa.field("reporting_entity_type", pa.string()),
        pa.field("last_updated_on", pa.string()),
        pa.field("version", pa.string()),
        pa.field("billing_code", pa.string()),
        pa.field("billing_code_type", pa.string()),
        pa.field("billing_code_type_version", pa.string()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("negotiation_arrangement", pa.string()),
        pa.field("severity_of_illness", pa.string()),
        pa.field("provider_references", pa.string()),
        pa.field("negotiated_type", pa.string()),
        pa.field("negotiated_rate", pa.float64()),
        pa.field("expiration_date", pa.string()),
        pa.field("billing_class", pa.string()),
        pa.field("service_code", pa.string()),
        pa.field("billing_code_modifier", pa.string()),
        pa.field("additional_information", pa.string()),
        pa.field("estimated_amount", pa.float64()),
        pa.field("setting", pa.string()),
        *LINEAGE_FIELDS,
    ]
)


TIC_OUT_OF_NETWORK_ALLOWED_SCHEMA = pa.schema(
    [
        pa.field("source_pricing_file", pa.string()),
        pa.field("reporting_entity_name", pa.string()),
        pa.field("reporting_entity_type", pa.string()),
        pa.field("last_updated_on", pa.string()),
        pa.field("version", pa.string()),
        pa.field("billing_code", pa.string()),
        pa.field("billing_code_type", pa.string()),
        pa.field("billing_code_type_version", pa.string()),
        pa.field("name", pa.string()),
        pa.field("description", pa.string()),
        pa.field("tin_type", pa.string()),
        pa.field("tin_value", pa.string()),
        pa.field("service_code", pa.string()),
        pa.field("billing_class", pa.string()),
        pa.field("allowed_amount", pa.float64()),
        pa.field("billed_charge", pa.float64()),
        pa.field("npi", pa.string()),
    ]
)
