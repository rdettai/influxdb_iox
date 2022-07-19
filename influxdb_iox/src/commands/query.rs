use influxdb_iox_client::{
    connection::Connection,
    flight::{self, generated_types::ReadInfo, generated_types::read_info},
    format::QueryOutputFormat,
};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error formatting: {0}")]
    Formatting(#[from] influxdb_iox_client::format::Error),

    #[error("Error querying: {0}")]
    Query(#[from] influxdb_iox_client::flight::Error),

    /// Unknown Query type type
    #[error("Unknown query type: {}. Expected one of 'sql' or 'ast'", .0)]
    InvalidQueryType(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Query the data with SQL
#[derive(Debug, clap::Parser)]
pub struct Config {
    /// Optional query type ('sql', or 'json')
    #[clap(short, long, default_value = "sql")]
    query_type: String,

    /// The IOx namespace to query
    #[clap(action)]
    namespace: String,

    /// The query to run, in either SQL or JSON AST format
    #[clap(action)]
    query: String,

    /// Optional format ('pretty', 'json', or 'csv')
    #[clap(short, long, default_value = "pretty", action)]
    format: String,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum QueryType {
    Sql,
    Ast
}

impl FromStr for QueryType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "sql" => Ok(Self::Sql),
            "ast" => Ok(Self::Ast),
            _ => Err(Error::InvalidQueryType(s.to_string())),
        }
    }
}

pub async fn command(connection: Connection, config: Config) -> Result<()> {
    let mut client = flight::Client::new(connection);
    let Config {
        query_type,
        namespace,
        format,
        query,
    } = config;

    let format = QueryOutputFormat::from_str(&format)?;
    let query_type = QueryType::from_str(&query_type)?;

    let mut query_results = client
        .perform_query(ReadInfo {
            namespace_name: namespace,
            query: Some(match query_type {
                QueryType::Sql => {
                    read_info::Query::Sql(query)
                },

                QueryType::Ast => {
                    read_info::Query::AstStatement(query)
                },
            }),
        })
        .await?;

    // It might be nice to do some sort of streaming write
    // rather than buffering the whole thing.
    let mut batches = vec![];
    while let Some(data) = query_results.next().await? {
        batches.push(data);
    }

    let formatted_result = format.format(&batches)?;

    println!("{}", formatted_result);

    Ok(())
}
