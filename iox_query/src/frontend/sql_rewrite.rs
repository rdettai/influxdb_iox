use sqlparser::ast::{
    Expr, Fetch, Function, FunctionArg, FunctionArgExpr, Ident, LockType, ObjectName, Offset,
    OrderByExpr, Query, Select, SelectItem, SetExpr, Statement, WindowSpec, With,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub fn rewrite_sql(sql: &str) -> String {
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, sql) {
        Ok(statements) if statements.len() == 1 => {
            let statement = statements.into_iter().next().unwrap();
            let rewritten = RewriteSpecial {}.rewrite_statement(statement);
            rewritten.to_string()
        }
        _ => sql.to_string(),
    }
}

// THIS IS NOT COMPLETE -- would need to fill out the rest of the code

/// Rewrites "special" SQL functions (that were created like `user`
/// rather than `user()`) into normal quoted column references like "user"
///
/// This is important in IOx for functions like `user` which are
/// also common column names in default telegraph data (e..g
///
/// ```sql
/// select user, time from cpu;
/// ```
/// To select the amount of time the CPU spent on user queries
struct RewriteSpecial {}

/// Override only the part we care about
impl Rewriter for RewriteSpecial {
    fn rewrite_expr(&mut self, e: Expr) -> Expr {
        if let Expr::Function(f) = e {
            if f.special {
                assert!(f.args.is_empty());
                assert!(f.over.is_none());
                let mut names = f.name.0;
                assert_eq!(names.len(), 1);
                let mut name = names.pop().unwrap();
                // ensure it is quoted on the output
                name.quote_style = Some('"');
                rewrite_expr(self, Expr::Identifier(name))
            } else {
                Expr::Function(rewrite_function(self, f))
            }
        } else {
            rewrite_expr(self, e)
        }
    }
}

/// Rewrites SQL trees recursively
///
/// If you override this, you should do your mutation first and then
/// call the default implementation to complete the recursion
pub trait Rewriter {
    fn rewrite_statements(&mut self, s: Vec<Statement>) -> Vec<Statement> {
        s.into_iter().map(|s| self.rewrite_statement(s)).collect()
    }

    fn rewrite_statement(&mut self, s: Statement) -> Statement {
        match s {
            Statement::Query(query) => Statement::Query(Box::new(self.rewrite_query(*query))),
            s => s,
        }
    }

    fn rewrite_query(&mut self, q: Query) -> Query {
        let Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
            lock,
        } = q;

        Query {
            with: with.map(|with| self.rewrite_with(with)),
            body: Box::new(self.rewrite_set_expr(*body)),
            order_by: order_by
                .into_iter()
                .map(|o| self.rewrite_order_by(o))
                .collect(),
            limit: limit.map(|limit| self.rewrite_expr(limit)),
            offset: offset.map(|offset| self.rewrite_offset(offset)),
            fetch: fetch.map(|fetch| self.rewrite_fetch(fetch)),
            lock: lock.map(|lock_type| self.rewrite_lock_type(lock_type)),
        }
    }

    fn rewrite_with(&mut self, w: With) -> With {
        w
    }

    fn rewrite_set_expr(&mut self, e: SetExpr) -> SetExpr {
        match e {
            SetExpr::Select(select) => SetExpr::Select(Box::new(self.rewrite_select(*select))),
            e => e,
        }
    }

    fn rewrite_select(&mut self, s: Select) -> Select {
        let Select {
            distinct,
            top,
            projection,
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            qualify,
        } = s;

        // TOOD rewrite all these fields

        Select {
            distinct,
            top,
            projection: projection
                .into_iter()
                .map(|s| self.rewrite_select_item(s))
                .collect(),
            into,
            from,
            lateral_views,
            selection,
            group_by,
            cluster_by,
            distribute_by,
            sort_by,
            having,
            qualify,
        }
    }

    fn rewrite_select_item(&mut self, e: SelectItem) -> SelectItem {
        match e {
            SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(self.rewrite_expr(expr)),
            SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
                expr: self.rewrite_expr(expr),
                alias: self.rewrite_ident(alias),
            },
            SelectItem::QualifiedWildcard(o) => {
                SelectItem::QualifiedWildcard(self.rewrite_object_name(o))
            }
            SelectItem::Wildcard => SelectItem::Wildcard,
        }
    }

    /// low level identifier rewriter -- recursion stops here
    fn rewrite_ident(&mut self, e: Ident) -> Ident {
        e
    }

    fn rewrite_object_name(&mut self, e: ObjectName) -> ObjectName {
        let ObjectName(names) = e;
        ObjectName(names.into_iter().map(|i| self.rewrite_ident(i)).collect())
    }

    fn rewrite_order_by(&mut self, e: OrderByExpr) -> OrderByExpr {
        e
    }

    fn rewrite_expr(&mut self, e: Expr) -> Expr {
        rewrite_expr(self, e)
    }

    fn rewrite_function(&mut self, f: Function) -> Function {
        rewrite_function(self, f)
    }

    fn rewrite_function_arg(&mut self, e: FunctionArg) -> FunctionArg {
        match e {
            FunctionArg::Named { name, arg } => FunctionArg::Named {
                name: self.rewrite_ident(name),
                arg: self.rewrite_function_arg_expr(arg),
            },
            FunctionArg::Unnamed(arg) => FunctionArg::Unnamed(self.rewrite_function_arg_expr(arg)),
        }
    }

    fn rewrite_function_arg_expr(&mut self, e: FunctionArgExpr) -> FunctionArgExpr {
        match e {
            FunctionArgExpr::Expr(e) => FunctionArgExpr::Expr(self.rewrite_expr(e)),
            FunctionArgExpr::QualifiedWildcard(o) => {
                FunctionArgExpr::QualifiedWildcard(self.rewrite_object_name(o))
            }
            FunctionArgExpr::Wildcard => FunctionArgExpr::Wildcard,
        }
    }

    fn rewrite_window_spec(&mut self, e: WindowSpec) -> WindowSpec {
        e
    }

    fn rewrite_offset(&mut self, e: Offset) -> Offset {
        e
    }

    fn rewrite_fetch(&mut self, e: Fetch) -> Fetch {
        e
    }

    fn rewrite_lock_type(&mut self, e: LockType) -> LockType {
        e
    }
}

// free function that does the work, rather than a default impl
fn rewrite_expr<R: Rewriter + ?Sized>(rewriter: &mut R, e: Expr) -> Expr {
    match e {
        Expr::Identifier(i) => Expr::Identifier(rewriter.rewrite_ident(i)),
        Expr::Function(f) => Expr::Function(rewriter.rewrite_function(f)),
        e => e,
    }
}

fn rewrite_function<R: Rewriter + ?Sized>(rewriter: &mut R, f: Function) -> Function {
    let Function {
        name,
        args,
        over,
        distinct,
        special,
    } = f;

    Function {
        name: rewriter.rewrite_object_name(name),
        args: args
            .into_iter()
            .map(|a| rewriter.rewrite_function_arg(a))
            .collect(),
        over: over.map(|o| rewriter.rewrite_window_spec(o)),
        distinct,
        special,
    }
}
