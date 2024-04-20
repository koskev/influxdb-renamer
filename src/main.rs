use std::collections::HashMap;

use chrono::DateTime;
use clap::Parser;
use influxdb::{Client, Error, InfluxDbWriteable, ReadQuery, Timestamp, WriteQuery};
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

#[derive(Serialize, Deserialize, Debug)]
struct DbEntry {
    #[serde(flatten)]
    data: HashMap<String, Value>,
}

#[derive(Debug)]
struct LineEntry {
    /// Name of the measurment
    name: String,
    time: u128,
    tags: HashMap<String, Value>,
    fields: HashMap<String, (Value, FieldType)>,
}

impl LineEntry {
    fn new(
        name: &str,
        tag_entries: &[TagEntry],
        field_entries: &[FieldEntry],
        data: &HashMap<String, Value>,
    ) -> Self {
        let mut tags = HashMap::new();
        let mut fields = HashMap::new();
        let mut time = 0;

        for (name, value) in data {
            if tag_entries.contains(&TagEntry {
                tag_key: name.clone(),
            }) {
                tags.insert(name.to_string(), value.clone());
            } else if let Some(entry) = field_entries.iter().find(|e| e.field_key == *name) {
                fields.insert(name.to_string(), (value.clone(), entry.field_type));
            } else if name == "time" {
                let date = DateTime::parse_from_rfc3339(value.as_str().unwrap()).unwrap();
                time = date.timestamp_nanos_opt().unwrap_or(0) as u128;
            } else {
                error!("Value with name {} is neither field or tag!", name);
            }
        }

        Self {
            name: name.into(),
            time,
            tags,
            fields,
        }
    }

    fn get_query(&self) -> WriteQuery {
        let mut query = Timestamp::Nanoseconds(self.time).into_query(&self.name);

        for (tag_name, tag_val) in &self.tags {
            query = query.add_tag(tag_name, tag_val.as_str());
        }

        for (field_name, (field_val, field_type)) in &self.fields {
            debug!("Adding field {} with val {}", field_name, field_val);
            match field_type {
                FieldType::Float => {
                    query = query.add_field(field_name, field_val.as_f64());
                }
                FieldType::Integer => {
                    query = query.add_field(field_name, field_val.as_u64());
                }
                FieldType::String => {
                    query = query.add_field(field_name, field_val.as_str());
                }
                FieldType::Boolean => {
                    query = query.add_field(field_name, field_val.as_bool());
                }
            }
        }
        query
    }
}

struct DatabaseRename {
    host: String,
    token: String,
    bucket: String,
    measurement: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
struct TagEntry {
    #[serde(rename = "tagKey")]
    tag_key: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum FieldType {
    Float,
    Integer,
    String,
    Boolean,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct FieldEntry {
    #[serde(rename = "fieldKey")]
    field_key: String,

    #[serde(rename = "fieldType")]
    field_type: FieldType,
}

impl DatabaseRename {
    async fn rename_tag(
        &self,
        tag_name: &str,
        old_val: &str,
        new_val: &str,
        _batch_size: usize,
    ) -> Result<(), Error> {
        let client = Client::new(&self.host, &self.bucket).with_token(&self.token);
        let read_query = ReadQuery::new(format!(
            "SELECT * FROM {} WHERE (\"{}\"::tag = '{}')",
            self.measurement, tag_name, old_val
        ));

        let tag_query = ReadQuery::new(format!("SHOW TAG KEYS FROM {}", self.measurement));
        let field_query = ReadQuery::new(format!("SHOW FIELD KEYS FROM {}", self.measurement));

        let mut tag_str = client.json_query(tag_query).await?;
        let mut field_str = client.json_query(field_query).await?;

        let tags: Vec<TagEntry> = tag_str
            .deserialize_next::<TagEntry>()?
            .series
            .into_iter()
            .flat_map(|e| e.values)
            .collect();

        let fields: Vec<FieldEntry> = field_str
            .deserialize_next::<FieldEntry>()?
            .series
            .into_iter()
            .flat_map(|e| e.values)
            .collect();

        let mut res = client.json_query(read_query).await?;
        // XXX: this might fill the memory
        // TODO: deserialize_next_tagged does not work for me and there is no documentation on how
        // to use it :/
        for series in res.deserialize_next::<DbEntry>()?.series {
            //let mut queries = Vec::with_capacity(batch_size);
            for (idx, entry) in series.values.iter().enumerate() {
                let mut new_line = LineEntry::new(&self.measurement, &tags, &fields, &entry.data);
                new_line
                    .tags
                    .entry(tag_name.into())
                    .and_modify(|e| *e = new_val.into());
                client.query(new_line.get_query()).await?;
                // TODO: Batching is very! fast, but does not seem to work
                //queries.push(new_line.get_query());
                //if queries.len() >= batch_size {
                //    client.query(queries.clone()).await?;
                //    queries.clear();
                //}
                info!("Wrote entry {}/{}", idx + 1, series.values.len());
            }
        }

        // TODO: delete old?

        Ok(())
    }
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Host to connect to. E.g. http://localhost:8086
    #[arg(long)]
    host: String,
    /// Access token
    #[arg(long)]
    token: String,
    /// Bucket the target is in
    #[arg(short, long)]
    bucket: String,
    /// The measurement to use
    #[arg(short, long)]
    measurement: String,
    /// The tag to use
    #[arg(long)]
    tag: String,
    /// The old value
    #[arg(short, long)]
    old_name: String,
    /// The new value
    #[arg(short, long)]
    new_name: String,
    /// Number of queries to batch (currently ignored)
    #[arg(long, default_value_t = 1000)]
    batch_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    TermLogger::init(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Stdout,
        ColorChoice::Auto,
    )
    .unwrap();
    let args = Cli::parse();
    let db = DatabaseRename {
        host: args.host,
        token: args.token,
        bucket: args.bucket,
        measurement: args.measurement,
    };
    db.rename_tag(&args.tag, &args.old_name, &args.new_name, args.batch_size)
        .await?;

    Ok(())
}
