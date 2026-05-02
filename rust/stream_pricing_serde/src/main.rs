use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Utc};
use chrono_tz::America::Chicago;
use clap::Parser;
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::Deserializer;
use uuid::Uuid;

const BATCH_SIZE: usize = 10_000;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    file: PathBuf,
    #[arg(long)]
    source_url: String,
    #[arg(long, default_value = "data/parquet")]
    output: PathBuf,
    #[arg(long)]
    target_npis_file: Option<PathBuf>,
    #[arg(long, default_value = "UHC")]
    payer_code: String,
    #[arg(long)]
    file_month: Option<String>,
    #[arg(long, default_value = "TX")]
    state: String,
    #[arg(long, default_value = "")]
    source_version: String,
    #[arg(long)]
    etl_run_id: Option<String>,
}

#[derive(Clone, Debug)]
struct Lineage {
    payer_code: String,
    file_month: String,
    state: String,
    source_version: String,
    etl_run_id: String,
    ingested_at: String,
}

#[derive(Default, Clone, Debug)]
struct Header {
    reporting_entity_name: String,
    reporting_entity_type: String,
    last_updated_on: String,
    version: String,
}

#[derive(Default, Deserialize)]
struct Tin {
    #[serde(default, rename = "type")]
    tin_type: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    business_name: String,
}

#[derive(Default, Deserialize)]
struct ProviderGroup {
    #[serde(default)]
    tin: Tin,
    #[serde(default)]
    npi: Vec<String>,
}

#[derive(Default, Deserialize)]
struct ProviderReference {
    #[serde(default)]
    provider_group_id: Option<i64>,
    #[serde(default)]
    network_name: Vec<String>,
    #[serde(default)]
    provider_groups: Vec<ProviderGroup>,
}

#[derive(Default, Deserialize)]
struct InNetworkItem {
    #[serde(default)]
    billing_code: String,
    #[serde(default)]
    billing_code_type: String,
    #[serde(default)]
    billing_code_type_version: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    negotiation_arrangement: String,
    #[serde(default)]
    severity_of_illness: String,
    #[serde(default)]
    negotiated_rates: Vec<NegotiatedRate>,
}

#[derive(Default, Deserialize)]
struct NegotiatedRate {
    #[serde(default)]
    provider_references: Vec<i64>,
    #[serde(default)]
    negotiated_prices: Vec<NegotiatedPrice>,
}

#[derive(Default, Deserialize)]
struct NegotiatedPrice {
    #[serde(default)]
    negotiated_type: String,
    #[serde(default)]
    negotiated_rate: Option<f64>,
    #[serde(default)]
    expiration_date: String,
    #[serde(default)]
    billing_class: String,
    #[serde(default)]
    service_code: Vec<String>,
    #[serde(default)]
    billing_code_modifier: Vec<String>,
    #[serde(default)]
    additional_information: String,
    #[serde(default)]
    estimated_amount: Option<f64>,
    #[serde(default)]
    setting: String,
}

#[derive(Clone)]
struct ContextState {
    source_url: String,
    lineage: Lineage,
    target_npis: Option<HashSet<String>>,
}

#[derive(Default)]
struct Counts {
    provider_rows: usize,
    price_rows: usize,
    refs_scanned: usize,
    items_scanned: usize,
}

struct ProviderWriter {
    writer: ArrowWriter<File>,
    source_pricing_file: StringBuilder,
    provider_group_id: Int64Builder,
    network_name: StringBuilder,
    tin_type: StringBuilder,
    tin_value: StringBuilder,
    npi: StringBuilder,
    business_name: StringBuilder,
    payer_code: StringBuilder,
    file_month: StringBuilder,
    state: StringBuilder,
    source_version: StringBuilder,
    etl_run_id: StringBuilder,
    ingested_at: StringBuilder,
    rows: usize,
    schema: Arc<Schema>,
}

impl ProviderWriter {
    fn new(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("source_pricing_file", DataType::Utf8, true),
            Field::new("provider_group_id", DataType::Int64, true),
            Field::new("network_name", DataType::Utf8, true),
            Field::new("tin_type", DataType::Utf8, true),
            Field::new("tin_value", DataType::Utf8, true),
            Field::new("npi", DataType::Utf8, true),
            Field::new("business_name", DataType::Utf8, true),
            Field::new("payer_code", DataType::Utf8, true),
            Field::new("file_month", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("source_version", DataType::Utf8, true),
            Field::new("etl_run_id", DataType::Utf8, true),
            Field::new("ingested_at", DataType::Utf8, true),
        ]));
        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        Ok(Self {
            writer,
            source_pricing_file: StringBuilder::new(),
            provider_group_id: Int64Builder::new(),
            network_name: StringBuilder::new(),
            tin_type: StringBuilder::new(),
            tin_value: StringBuilder::new(),
            npi: StringBuilder::new(),
            business_name: StringBuilder::new(),
            payer_code: StringBuilder::new(),
            file_month: StringBuilder::new(),
            state: StringBuilder::new(),
            source_version: StringBuilder::new(),
            etl_run_id: StringBuilder::new(),
            ingested_at: StringBuilder::new(),
            rows: 0,
            schema,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        ctx: &ContextState,
        provider_group_id: Option<i64>,
        network_name: &str,
        tin_type: &str,
        tin_value: &str,
        npi: &str,
        business_name: &str,
    ) -> Result<()> {
        self.source_pricing_file.append_value(&ctx.source_url);
        match provider_group_id {
            Some(value) => self.provider_group_id.append_value(value),
            None => self.provider_group_id.append_null(),
        }
        self.network_name.append_value(network_name);
        self.tin_type.append_value(tin_type);
        self.tin_value.append_value(tin_value);
        self.npi.append_value(npi);
        self.business_name.append_value(business_name);
        append_lineage_provider(self, &ctx.lineage);
        self.rows += 1;
        if self.rows >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.source_pricing_file.finish()),
                Arc::new(self.provider_group_id.finish()),
                Arc::new(self.network_name.finish()),
                Arc::new(self.tin_type.finish()),
                Arc::new(self.tin_value.finish()),
                Arc::new(self.npi.finish()),
                Arc::new(self.business_name.finish()),
                Arc::new(self.payer_code.finish()),
                Arc::new(self.file_month.finish()),
                Arc::new(self.state.finish()),
                Arc::new(self.source_version.finish()),
                Arc::new(self.etl_run_id.finish()),
                Arc::new(self.ingested_at.finish()),
            ],
        )?;
        self.writer.write(&batch)?;
        self.rows = 0;
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}

fn append_lineage_provider(writer: &mut ProviderWriter, lineage: &Lineage) {
    writer.payer_code.append_value(&lineage.payer_code);
    writer.file_month.append_value(&lineage.file_month);
    writer.state.append_value(&lineage.state);
    writer.source_version.append_value(&lineage.source_version);
    writer.etl_run_id.append_value(&lineage.etl_run_id);
    writer.ingested_at.append_value(&lineage.ingested_at);
}

struct PriceWriter {
    writer: ArrowWriter<File>,
    source_pricing_file: StringBuilder,
    reporting_entity_name: StringBuilder,
    reporting_entity_type: StringBuilder,
    last_updated_on: StringBuilder,
    version: StringBuilder,
    billing_code: StringBuilder,
    billing_code_type: StringBuilder,
    billing_code_type_version: StringBuilder,
    name: StringBuilder,
    description: StringBuilder,
    negotiation_arrangement: StringBuilder,
    severity_of_illness: StringBuilder,
    provider_references: StringBuilder,
    negotiated_type: StringBuilder,
    negotiated_rate: Float64Builder,
    expiration_date: StringBuilder,
    billing_class: StringBuilder,
    service_code: StringBuilder,
    billing_code_modifier: StringBuilder,
    additional_information: StringBuilder,
    estimated_amount: Float64Builder,
    setting: StringBuilder,
    payer_code: StringBuilder,
    file_month: StringBuilder,
    state: StringBuilder,
    source_version: StringBuilder,
    etl_run_id: StringBuilder,
    ingested_at: StringBuilder,
    rows: usize,
    schema: Arc<Schema>,
}

impl PriceWriter {
    fn new(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("source_pricing_file", DataType::Utf8, true),
            Field::new("reporting_entity_name", DataType::Utf8, true),
            Field::new("reporting_entity_type", DataType::Utf8, true),
            Field::new("last_updated_on", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("billing_code", DataType::Utf8, true),
            Field::new("billing_code_type", DataType::Utf8, true),
            Field::new("billing_code_type_version", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("negotiation_arrangement", DataType::Utf8, true),
            Field::new("severity_of_illness", DataType::Utf8, true),
            Field::new("provider_references", DataType::Utf8, true),
            Field::new("negotiated_type", DataType::Utf8, true),
            Field::new("negotiated_rate", DataType::Float64, true),
            Field::new("expiration_date", DataType::Utf8, true),
            Field::new("billing_class", DataType::Utf8, true),
            Field::new("service_code", DataType::Utf8, true),
            Field::new("billing_code_modifier", DataType::Utf8, true),
            Field::new("additional_information", DataType::Utf8, true),
            Field::new("estimated_amount", DataType::Float64, true),
            Field::new("setting", DataType::Utf8, true),
            Field::new("payer_code", DataType::Utf8, true),
            Field::new("file_month", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
            Field::new("source_version", DataType::Utf8, true),
            Field::new("etl_run_id", DataType::Utf8, true),
            Field::new("ingested_at", DataType::Utf8, true),
        ]));
        let file = File::create(path)?;
        let writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        Ok(Self {
            writer,
            source_pricing_file: StringBuilder::new(),
            reporting_entity_name: StringBuilder::new(),
            reporting_entity_type: StringBuilder::new(),
            last_updated_on: StringBuilder::new(),
            version: StringBuilder::new(),
            billing_code: StringBuilder::new(),
            billing_code_type: StringBuilder::new(),
            billing_code_type_version: StringBuilder::new(),
            name: StringBuilder::new(),
            description: StringBuilder::new(),
            negotiation_arrangement: StringBuilder::new(),
            severity_of_illness: StringBuilder::new(),
            provider_references: StringBuilder::new(),
            negotiated_type: StringBuilder::new(),
            negotiated_rate: Float64Builder::new(),
            expiration_date: StringBuilder::new(),
            billing_class: StringBuilder::new(),
            service_code: StringBuilder::new(),
            billing_code_modifier: StringBuilder::new(),
            additional_information: StringBuilder::new(),
            estimated_amount: Float64Builder::new(),
            setting: StringBuilder::new(),
            payer_code: StringBuilder::new(),
            file_month: StringBuilder::new(),
            state: StringBuilder::new(),
            source_version: StringBuilder::new(),
            etl_run_id: StringBuilder::new(),
            ingested_at: StringBuilder::new(),
            rows: 0,
            schema,
        })
    }

    fn append(&mut self, ctx: &ContextState, header: &Header, item: &InNetworkItem, rate: &NegotiatedRate, price: Option<&NegotiatedPrice>) -> Result<()> {
        self.source_pricing_file.append_value(&ctx.source_url);
        self.reporting_entity_name.append_value(&header.reporting_entity_name);
        self.reporting_entity_type.append_value(&header.reporting_entity_type);
        self.last_updated_on.append_value(&header.last_updated_on);
        self.version.append_value(&header.version);
        self.billing_code.append_value(&item.billing_code);
        self.billing_code_type.append_value(&item.billing_code_type);
        self.billing_code_type_version.append_value(&item.billing_code_type_version);
        self.name.append_value(&item.name);
        self.description.append_value(&item.description);
        self.negotiation_arrangement.append_value(&item.negotiation_arrangement);
        self.severity_of_illness.append_value(&item.severity_of_illness);
        self.provider_references.append_value(join_i64(&rate.provider_references));
        if let Some(price) = price {
            self.negotiated_type.append_value(&price.negotiated_type);
            append_f64(&mut self.negotiated_rate, price.negotiated_rate);
            self.expiration_date.append_value(&price.expiration_date);
            self.billing_class.append_value(&price.billing_class);
            self.service_code.append_value(join_str(&price.service_code));
            self.billing_code_modifier.append_value(join_str(&price.billing_code_modifier));
            self.additional_information.append_value(&price.additional_information);
            append_f64(&mut self.estimated_amount, price.estimated_amount);
            self.setting.append_value(&price.setting);
        } else {
            self.negotiated_type.append_value("");
            self.negotiated_rate.append_null();
            self.expiration_date.append_value("");
            self.billing_class.append_value("");
            self.service_code.append_value("");
            self.billing_code_modifier.append_value("");
            self.additional_information.append_value("");
            self.estimated_amount.append_null();
            self.setting.append_value("");
        }
        self.payer_code.append_value(&ctx.lineage.payer_code);
        self.file_month.append_value(&ctx.lineage.file_month);
        self.state.append_value(&ctx.lineage.state);
        self.source_version.append_value(&ctx.lineage.source_version);
        self.etl_run_id.append_value(&ctx.lineage.etl_run_id);
        self.ingested_at.append_value(&ctx.lineage.ingested_at);
        self.rows += 1;
        if self.rows >= BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.source_pricing_file.finish()),
                Arc::new(self.reporting_entity_name.finish()),
                Arc::new(self.reporting_entity_type.finish()),
                Arc::new(self.last_updated_on.finish()),
                Arc::new(self.version.finish()),
                Arc::new(self.billing_code.finish()),
                Arc::new(self.billing_code_type.finish()),
                Arc::new(self.billing_code_type_version.finish()),
                Arc::new(self.name.finish()),
                Arc::new(self.description.finish()),
                Arc::new(self.negotiation_arrangement.finish()),
                Arc::new(self.severity_of_illness.finish()),
                Arc::new(self.provider_references.finish()),
                Arc::new(self.negotiated_type.finish()),
                Arc::new(self.negotiated_rate.finish()),
                Arc::new(self.expiration_date.finish()),
                Arc::new(self.billing_class.finish()),
                Arc::new(self.service_code.finish()),
                Arc::new(self.billing_code_modifier.finish()),
                Arc::new(self.additional_information.finish()),
                Arc::new(self.estimated_amount.finish()),
                Arc::new(self.setting.finish()),
                Arc::new(self.payer_code.finish()),
                Arc::new(self.file_month.finish()),
                Arc::new(self.state.finish()),
                Arc::new(self.source_version.finish()),
                Arc::new(self.etl_run_id.finish()),
                Arc::new(self.ingested_at.finish()),
            ],
        )?;
        self.writer.write(&batch)?;
        self.rows = 0;
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}

fn append_f64(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn join_str(values: &[String]) -> String {
    values.iter().filter(|v| !v.is_empty()).cloned().collect::<Vec<_>>().join("|")
}

fn join_i64(values: &[i64]) -> String {
    values.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("|")
}

struct RootSeed<'a> {
    ctx: &'a mut ContextState,
    provider_writer: &'a mut ProviderWriter,
    price_writer: &'a mut PriceWriter,
    counts: &'a mut Counts,
    matched_group_ids: &'a mut HashSet<i64>,
}

impl<'de, 'a> DeserializeSeed<'de> for RootSeed<'a> {
    type Value = Header;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RootVisitor { seed: self, header: Header::default() })
    }
}

struct RootVisitor<'a> {
    seed: RootSeed<'a>,
    header: Header,
}

impl<'de, 'a> Visitor<'de> for RootVisitor<'a> {
    type Value = Header;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a TiC pricing JSON object")
    }

    fn visit_map<M>(mut self, mut map: M) -> std::result::Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "reporting_entity_name" => self.header.reporting_entity_name = map.next_value::<Option<String>>()?.unwrap_or_default(),
                "reporting_entity_type" => self.header.reporting_entity_type = map.next_value::<Option<String>>()?.unwrap_or_default(),
                "last_updated_on" => self.header.last_updated_on = map.next_value::<Option<String>>()?.unwrap_or_default(),
                "version" => {
                    self.header.version = map.next_value::<Option<String>>()?.unwrap_or_default();
                    if self.seed.ctx.lineage.source_version.is_empty() {
                        self.seed.ctx.lineage.source_version = self.header.version.clone();
                    }
                }
                "provider_references" => {
                    let seed = ProviderRefsSeed {
                        ctx: &*self.seed.ctx,
                        writer: &mut *self.seed.provider_writer,
                        counts: &mut *self.seed.counts,
                        matched_group_ids: &mut *self.seed.matched_group_ids,
                    };
                    map.next_value_seed(seed)?;
                }
                "in_network" => {
                    let seed = InNetworkSeed {
                        ctx: &*self.seed.ctx,
                        header: &self.header,
                        writer: &mut *self.seed.price_writer,
                        counts: &mut *self.seed.counts,
                        matched_group_ids: &*self.seed.matched_group_ids,
                    };
                    map.next_value_seed(seed)?;
                }
                _ => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(self.header)
    }
}

struct ProviderRefsSeed<'a> {
    ctx: &'a ContextState,
    writer: &'a mut ProviderWriter,
    counts: &'a mut Counts,
    matched_group_ids: &'a mut HashSet<i64>,
}

impl<'de, 'a> DeserializeSeed<'de> for ProviderRefsSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(ProviderRefsVisitor { seed: self })
    }
}

struct ProviderRefsVisitor<'a> {
    seed: ProviderRefsSeed<'a>,
}

impl<'de, 'a> Visitor<'de> for ProviderRefsVisitor<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("provider_references array")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(reference) = seq.next_element::<ProviderReference>()? {
            self.seed.counts.refs_scanned += 1;
            emit_provider_reference(&reference, self.seed.ctx, self.seed.writer, self.seed.matched_group_ids)
                .map_err(de::Error::custom)
                .map(|rows| self.seed.counts.provider_rows += rows)?;
            if self.seed.counts.refs_scanned % 5000 == 0 {
                eprintln!(
                    "provider refs={} provider_rows={} matched_groups={}",
                    self.seed.counts.refs_scanned,
                    self.seed.counts.provider_rows,
                    self.seed.matched_group_ids.len()
                );
            }
        }
        Ok(())
    }
}

fn emit_provider_reference(
    reference: &ProviderReference,
    ctx: &ContextState,
    writer: &mut ProviderWriter,
    matched_group_ids: &mut HashSet<i64>,
) -> Result<usize> {
    let network_name = join_str(&reference.network_name);
    let mut rows = 0;
    let mut item_has_target_npi = false;
    if let Some(target_npis) = &ctx.target_npis {
        item_has_target_npi = reference
            .provider_groups
            .iter()
            .flat_map(|group| group.npi.iter())
            .any(|npi| target_npis.contains(npi.trim()));
    }

    if reference.provider_groups.is_empty() {
        if ctx.target_npis.is_none() {
            writer.append(ctx, reference.provider_group_id, &network_name, "", "", "", "")?;
            rows += 1;
        }
        return Ok(rows);
    }

    for group in &reference.provider_groups {
        let npi_values: Vec<String> = if let Some(target_npis) = &ctx.target_npis {
            group
                .npi
                .iter()
                .filter(|npi| target_npis.contains(npi.trim()))
                .map(|npi| npi.trim().to_string())
                .collect()
        } else {
            group.npi.iter().map(|npi| npi.trim().to_string()).collect()
        };

        if let (true, Some(group_id)) = (!npi_values.is_empty(), reference.provider_group_id) {
            matched_group_ids.insert(group_id);
        }

        if ctx.target_npis.is_some() && npi_values.is_empty() && !item_has_target_npi {
            continue;
        }

        if group.npi.is_empty() {
            if ctx.target_npis.is_none() {
                writer.append(
                    ctx,
                    reference.provider_group_id,
                    &network_name,
                    &group.tin.tin_type,
                    &group.tin.value,
                    "",
                    &group.tin.business_name,
                )?;
                rows += 1;
            }
        } else {
            for npi in npi_values {
                writer.append(
                    ctx,
                    reference.provider_group_id,
                    &network_name,
                    &group.tin.tin_type,
                    &group.tin.value,
                    &npi,
                    &group.tin.business_name,
                )?;
                rows += 1;
            }
        }
    }
    Ok(rows)
}

struct InNetworkSeed<'a> {
    ctx: &'a ContextState,
    header: &'a Header,
    writer: &'a mut PriceWriter,
    counts: &'a mut Counts,
    matched_group_ids: &'a HashSet<i64>,
}

impl<'de, 'a> DeserializeSeed<'de> for InNetworkSeed<'a> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(InNetworkVisitor { seed: self })
    }
}

struct InNetworkVisitor<'a> {
    seed: InNetworkSeed<'a>,
}

impl<'de, 'a> Visitor<'de> for InNetworkVisitor<'a> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("in_network array")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(item) = seq.next_element::<InNetworkItem>()? {
            self.seed.counts.items_scanned += 1;
            emit_prices(
                &item,
                self.seed.ctx,
                self.seed.header,
                self.seed.writer,
                self.seed.matched_group_ids,
            )
            .map_err(de::Error::custom)
            .map(|rows| self.seed.counts.price_rows += rows)?;
            if self.seed.counts.items_scanned % 2000 == 0 {
                eprintln!(
                    "in_network_items={} price_rows={}",
                    self.seed.counts.items_scanned,
                    self.seed.counts.price_rows
                );
            }
        }
        Ok(())
    }
}

fn emit_prices(
    item: &InNetworkItem,
    ctx: &ContextState,
    header: &Header,
    writer: &mut PriceWriter,
    matched_group_ids: &HashSet<i64>,
) -> Result<usize> {
    let mut rows = 0;
    if item.negotiated_rates.is_empty() {
        if ctx.target_npis.is_none() {
            writer.append(ctx, header, item, &NegotiatedRate::default(), None)?;
            rows += 1;
        }
        return Ok(rows);
    }

    for rate in &item.negotiated_rates {
        if ctx.target_npis.is_some()
            && !rate.provider_references.iter().any(|id| matched_group_ids.contains(id))
        {
            continue;
        }
        if rate.negotiated_prices.is_empty() {
            writer.append(ctx, header, item, rate, None)?;
            rows += 1;
        } else {
            for price in &rate.negotiated_prices {
                writer.append(ctx, header, item, rate, Some(price))?;
                rows += 1;
            }
        }
    }
    Ok(rows)
}

fn source_name_from_url(url: &str) -> String {
    let without_query = url.split('?').next().unwrap_or(url);
    let name = without_query.rsplit('/').next().unwrap_or("pricing");
    name.trim_end_matches(".gz")
        .trim_end_matches(".json")
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' { ch } else { '_' })
        .collect()
}

fn open_reader(path: &Path) -> Result<Box<dyn Read>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    if path.extension().and_then(|value| value.to_str()).unwrap_or("").eq_ignore_ascii_case("gz") {
        Ok(Box::new(GzDecoder::new(reader)))
    } else {
        Ok(Box::new(reader))
    }
}

fn load_target_npis(path: Option<&Path>) -> Result<Option<HashSet<String>>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let text = std::fs::read_to_string(path)?;
    let values = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect();
    Ok(Some(values))
}

fn default_file_month() -> String {
    let now = Utc::now();
    format!("{:04}-{:02}", now.year(), now.month())
}

fn central_ingested_at() -> String {
    Utc::now().with_timezone(&Chicago).to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let source_name = source_name_from_url(&args.source_url);
    let provider_path = args.output.join("tic_provider_reference").join(format!("{source_name}.parquet"));
    let price_path = args.output.join("tic_price").join(format!("{source_name}.parquet"));

    let lineage = Lineage {
        payer_code: args.payer_code,
        file_month: args.file_month.unwrap_or_else(default_file_month),
        state: args.state,
        source_version: args.source_version,
        etl_run_id: args.etl_run_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
        ingested_at: central_ingested_at(),
    };
    let mut ctx = ContextState {
        source_url: args.source_url,
        lineage,
        target_npis: load_target_npis(args.target_npis_file.as_deref())?,
    };

    let mut provider_writer = ProviderWriter::new(&provider_path)?;
    let mut price_writer = PriceWriter::new(&price_path)?;
    let mut counts = Counts::default();
    let mut matched_group_ids = HashSet::new();

    let reader = open_reader(&args.file)?;
    let mut deserializer = Deserializer::from_reader(reader);
    let header = RootSeed {
        ctx: &mut ctx,
        provider_writer: &mut provider_writer,
        price_writer: &mut price_writer,
        counts: &mut counts,
        matched_group_ids: &mut matched_group_ids,
    }
    .deserialize(&mut deserializer)?;

    provider_writer.close()?;
    price_writer.close()?;

    eprintln!(
        "complete refs={} provider_rows={} matched_groups={} in_network_items={} price_rows={} source_version={}",
        counts.refs_scanned,
        counts.provider_rows,
        matched_group_ids.len(),
        counts.items_scanned,
        counts.price_rows,
        if ctx.lineage.source_version.is_empty() { header.version } else { ctx.lineage.source_version }
    );
    Ok(())
}
