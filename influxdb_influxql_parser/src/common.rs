#![allow(dead_code)]

use crate::identifier::{identifier, Identifier};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{line_ending, multispace0};
use nom::combinator::{eof, map, opt};
use nom::sequence::{delimited, pair, terminated, tuple};
use nom::IResult;

/// Represents a measurement expression of an InfluxQL statement.
///
/// A measurement expression can be either
///
/// * a 1, 2 or 3-part name; or
/// * a regular expression.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum MeasurementExpression {
    Name {
        database: Option<Identifier>,
        retention_policy: Option<Identifier>,
        name: Identifier,
    },
    // TODO(sgc): Implement regular expression
}

pub fn measurement_expression(i: &str) -> IResult<&str, MeasurementExpression> {
    let (remaining_input, (opt_db_rp, name)) = tuple((
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
    ))(i)?;

    // Extract possible `database` and / or `retention_policy`
    let (database, retention_policy) = match opt_db_rp {
        Some(db_rp) => db_rp,
        _ => (None, None),
    };

    Ok((
        remaining_input,
        MeasurementExpression::Name {
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
    fn test_measurement_expression() {
        let database = Identifier::Unquoted("telegraf".into());
        let retention_policy = Identifier::Unquoted("autogen".into());
        let name = Identifier::Unquoted("diskio".into());

        let (_, got) = measurement_expression("diskio").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Name {
                database: None,
                retention_policy: None,
                name: name.clone(),
            }
        );

        let (_, got) = measurement_expression("telegraf.autogen.diskio").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Name {
                database: Some(database.clone()),
                retention_policy: Some(retention_policy.clone()),
                name: name.clone(),
            }
        );

        let (_, got) = measurement_expression("telegraf..diskio").unwrap();
        assert_eq!(
            got,
            MeasurementExpression::Name {
                database: Some(database.clone()),
                retention_policy: None,
                name: name.clone(),
            }
        );
    }
}
