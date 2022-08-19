#![allow(dead_code)]

use crate::identifier::{identifier, Identifier};
use core::fmt;
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{line_ending, multispace0};
use nom::combinator::{eof, map, opt};
use nom::sequence::{delimited, pair, terminated};
use nom::IResult;
use std::fmt::Formatter;

/// Represents a fully-qualified measurement name.
///
/// A measurement expression can be either
///
/// * a 1, 2 or 3-part name; or
/// * a regular expression.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct MeasurementNameExpression {
    pub database: Option<Identifier>,
    pub retention_policy: Option<Identifier>,
    pub name: Identifier,
}

impl fmt::Display for MeasurementNameExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self {
                database: None,
                retention_policy: None,
                name,
            } => write!(f, "{}", name)?,
            Self {
                database: Some(db),
                retention_policy: None,
                name,
            } => write!(f, "{}..{}", db, name)?,
            Self {
                database: None,
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}", rp, name)?,
            Self {
                database: Some(db),
                retention_policy: Some(rp),
                name,
            } => write!(f, "{}.{}.{}", db, rp, name)?,
        };
        Ok(())
    }
}

/// Match a 3-part measurement name expression.
pub fn measurement_name_expression(i: &str) -> IResult<&str, MeasurementNameExpression> {
    let (remaining_input, (opt_db_rp, name)) = pair(
        opt(alt((
            // database "." retention_policy "."
            map(
                pair(
                    terminated(identifier, tag(".")),
                    terminated(identifier, tag(".")),
                ),
                |(db, rp)| (Some(db), Some(rp)),
            ),
            // database ".."
            map(terminated(identifier, tag("..")), |db| {
                (Some(db), None)
            }),
            // retention_policy "."
            map(terminated(identifier, tag(".")), |rp| {
                (None, Some(rp))
            }),
        ))),
        identifier,
    )(i)?;

    // Extract possible `database` and / or `retention_policy`
    let (database, retention_policy) = match opt_db_rp {
        Some(db_rp) => db_rp,
        _ => (None, None),
    };

    Ok((
        remaining_input,
        MeasurementNameExpression {
            database,
            retention_policy,
            name,
        },
    ))
}

// Parse a terminator that ends a SQL statement.
pub fn statement_terminator(i: &str) -> IResult<&str, ()> {
    let (remaining_input, _) =
        delimited(multispace0, alt((tag(";"), line_ending, eof)), multispace0)(i)?;

    Ok((remaining_input, ()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_measurement_name_expression() {
        let database = Identifier::Unquoted("telegraf".into());
        let retention_policy = Identifier::Unquoted("autogen".into());
        let name = Identifier::Unquoted("diskio".into());

        let (_, got) = measurement_name_expression("diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: None,
                retention_policy: None,
                name: name.clone(),
            }
        );

        let (_, got) = measurement_name_expression("telegraf.autogen.diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: Some(database.clone()),
                retention_policy: Some(retention_policy.clone()),
                name: name.clone(),
            }
        );

        let (_, got) = measurement_name_expression("telegraf..diskio").unwrap();
        assert_eq!(
            got,
            MeasurementNameExpression {
                database: Some(database.clone()),
                retention_policy: None,
                name: name.clone(),
            }
        );
    }
}
