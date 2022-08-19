//! Parse a [`SHOW MEASUREMENTS`][sql] statement.
//!
//! [sql]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-schema/#show-measurements

#![allow(dead_code)]

use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::multispace1;
use nom::combinator::{map, opt, value};
use nom::sequence::{pair, preceded, terminated};
use nom::{sequence::tuple, IResult};
use std::fmt;
use std::fmt::Formatter;

use crate::common::{measurement_name_expression, statement_terminator, MeasurementNameExpression};
use crate::identifier::{identifier, Identifier};

/// OnExpression represents an InfluxQL database or retention policy name
/// or a wildcard.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum OnExpression {
    Database(Identifier),
    DatabaseRetentionPolicy(Identifier, Identifier),
    AllDatabases,
    AllDatabasesAndRetentionPolicies,
}

impl fmt::Display for OnExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(db) => write!(f, "{}", db),
            Self::DatabaseRetentionPolicy(db, rp) => write!(f, "{}.{}", db, rp),
            Self::AllDatabases => write!(f, "*"),
            Self::AllDatabasesAndRetentionPolicies => write!(f, "*.*"),
        }
    }
}

/// Parse the `ON` expression of the `SHOW MEASUREMENTS` statement.
fn on_expression(i: &str) -> IResult<&str, OnExpression> {
    preceded(
        pair(tag_no_case("ON"), multispace1),
        alt((
            value(OnExpression::AllDatabasesAndRetentionPolicies, tag("*.*")),
            value(OnExpression::AllDatabases, tag("*")),
            map(
                pair(
                    opt(terminated(identifier, tag("."))),
                    identifier,
                ),
                |tup| match tup {
                    (None, db) => OnExpression::Database(db),
                    (Some(db), rp) => OnExpression::DatabaseRetentionPolicy(db, rp),
                },
            ),
        )),
    )(i)
}

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ShowMeasurementsStatement {
    pub on_expression: Option<OnExpression>,
    pub measurement_expression: Option<MeasurementExpression>,
}

impl fmt::Display for ShowMeasurementsStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SHOW MEASUREMENTS")?;

        if let Some(ref expr) = self.on_expression {
            write!(f, " ON {}", expr)?;
        }

        if let Some(ref expr) = self.measurement_expression {
            write!(f, " WITH MEASUREMENT {}", expr)?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MeasurementExpression {
    Equals(MeasurementNameExpression),
    // TODO(sgc): Add regex
}

impl fmt::Display for MeasurementExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Equals(ref name) => write!(f, "= {}", name)?,
        };

        Ok(())
    }
}

fn with_measurement_expression(i: &str) -> IResult<&str, MeasurementExpression> {
    preceded(
        tuple((
            tag_no_case("with"),
            multispace1,
            tag_no_case("measurement"),
            multispace1,
            // TODO(sgc): parse equality or regular expression
            tag("="),
            multispace1,
        )),
        map(measurement_name_expression, MeasurementExpression::Equals),
    )(i)
}

pub fn show_measurements(i: &str) -> IResult<&str, ShowMeasurementsStatement> {
    let (remaining_input, (_, _, _, on_expression, measurement_expression, _)) = tuple((
        tag_no_case("show"),
        multispace1,
        tag_no_case("measurements"),
        opt(preceded(multispace1, on_expression)),
        opt(preceded(multispace1, with_measurement_expression)),
        statement_terminator,
    ))(i)?;

    Ok((
        remaining_input,
        ShowMeasurementsStatement {
            on_expression,
            measurement_expression,
        },
    ))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_show_measurements() {
        let (_, got) = show_measurements("SHOW measurements;").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on_expression: None,
                ..Default::default()
            },
        );

        let (_, got) = show_measurements("SHOW measurements ON foo;").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::Database(Identifier::Unquoted("foo".into()))),
                ..Default::default()
            },
        );

        let (_, got) =
            show_measurements("SHOW  MEASUREMENTS  ON  foo  WITH  MEASUREMENT  =  bar;").unwrap();
        assert_eq!(
            got,
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::Database(Identifier::Unquoted("foo".into()))),
                measurement_expression: Some(MeasurementExpression::Equals(
                    MeasurementNameExpression {
                        database: None,
                        retention_policy: None,
                        name: Identifier::Unquoted("bar".into()),
                    }
                )),
            },
        );
        assert_eq!(
            got.to_string(),
            "SHOW MEASUREMENTS ON foo WITH MEASUREMENT = bar"
        );
    }

    #[test]
    fn test_display() {
        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: None,
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::Database(Identifier::Unquoted("foo".into()))),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON foo");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::DatabaseRetentionPolicy(
                    Identifier::Unquoted("foo".into()),
                    Identifier::Unquoted("bar".into())
                )),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON foo.bar");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::AllDatabases),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *");

        let got = format!(
            "{}",
            ShowMeasurementsStatement {
                on_expression: Some(OnExpression::AllDatabasesAndRetentionPolicies),
                ..Default::default()
            }
        );
        assert_eq!(got, "SHOW MEASUREMENTS ON *.*");
    }

    #[test]
    fn test_on_expression() {
        let (_, got) = on_expression("ON cpu").unwrap();
        assert!(matches!(got, OnExpression::Database(Identifier::Unquoted(db)) if db == "cpu"));

        let (_, got) = on_expression("ON cpu.autogen").unwrap();
        assert!(
            matches!(got, OnExpression::DatabaseRetentionPolicy(Identifier::Unquoted(db), Identifier::Unquoted(rp)) if db == "cpu" && rp == "autogen")
        );

        let (_, got) = on_expression("ON *").unwrap();
        assert!(matches!(got, OnExpression::AllDatabases));

        let (_, got) = on_expression("ON *.*").unwrap();
        assert!(matches!(
            got,
            OnExpression::AllDatabasesAndRetentionPolicies
        ));
    }
}
