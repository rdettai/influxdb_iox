use std::fmt::Debug;
use std::process::exit;
use sqlparser::{
    ast::{ Statement },
    dialect::{ GenericDialect },
    parser::{ Parser as SQLParser },
};

use clap::{ Parser , Subcommand};
use sqlparser::ast::Query;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Dump parsed SQL as JSON
    #[clap(arg_required_else_help = true)]
    Dump {
        #[clap(value_parser)]
        sql: String,
    },

    /// Validate JSON
    #[clap(arg_required_else_help = true)]
    Validate {
        #[clap(value_parser)]
        json: String,
    }
}

fn main() {
    let args: Cli = Cli::parse();

    match args.command {
        Commands::Dump { sql } => {
            let dialect = &GenericDialect {};
            let mut statements: Vec<Statement> = SQLParser::parse_sql(dialect, sql.as_str()).expect("failed to parse SLQ");
            if statements.len() != 1 {
                eprintln!("Unexpected number of statements");
                exit(1)
            }
            let stmt = match statements.pop() {
                Some(Statement::Query(qry)) => {
                    serde_json::to_string(&*qry)
                },
                _ => {
                    eprintln!("Unexpected statement");
                    exit(1)
                }
            }.expect("expected a single query");

            println!("{}", stmt);
        },

        Commands::Validate { json } => {
            let qry: Query = serde_json::from_str(json.as_str()).expect("unable to parse JSON");
            println!("{}", qry);
        }
    };
}
