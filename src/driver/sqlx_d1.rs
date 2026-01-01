use futures_util::lock::Mutex;
use sea_query::Values;
use sea_query_sqlx::SqlxValues;
use sqlx::IntoArguments as _;
use std::{future::Future, pin::Pin, sync::Arc};

use tracing::{instrument, warn};

use crate::{
    AccessMode, ConnectOptions, DatabaseConnection, DatabaseConnectionType, DatabaseTransaction,
    IsolationLevel, QueryStream, Statement, TransactionError, debug_print, error::*,
    executor::*,
};

// NOTE: This module is compiled under `feature = "cloudflare-d1"` and must not rely on the
// internal `sqlx-dep` feature gate. Keep any shared SQLx helpers we need local to this module.

pub(crate) fn d1_sqlx_error_to_exec_err(err: sqlx::Error) -> DbErr {
    // `RuntimeErr::SqlxError` is behind `feature = "sqlx-dep"`. For the D1 driver we keep
    // error plumbing runtime-independent by falling back to a string-based internal error.
    DbErr::Exec(crate::RuntimeErr::Internal(err.to_string()))
}

pub(crate) fn d1_sqlx_error_to_query_err(err: sqlx::Error) -> DbErr {
    DbErr::Query(crate::RuntimeErr::Internal(err.to_string()))
}

pub(crate) fn d1_sqlx_error_to_conn_err(err: sqlx::Error) -> DbErr {
    DbErr::Conn(crate::RuntimeErr::Internal(err.to_string()))
}

type D1QueryResult = <::sqlx_d1::D1 as sqlx::Database>::QueryResult;

fn split_unprepared_sql(sql: &str) -> Vec<&str> {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        LineComment,
        BlockComment,
    }

    let mut state = State::Normal;
    let mut start = 0usize;
    let mut i = 0usize;
    let bytes = sql.as_bytes();
    let mut out: Vec<&str> = Vec::new();

    while i < bytes.len() {
        match state {
            State::Normal => match bytes[i] {
                b'\'' => {
                    state = State::SingleQuote;
                    i += 1;
                }
                b'"' => {
                    state = State::DoubleQuote;
                    i += 1;
                }
                b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                    state = State::LineComment;
                    i += 2;
                }
                b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => {
                    state = State::BlockComment;
                    i += 2;
                }
                b';' => {
                    let stmt = &sql[start..i];
                    if !stmt.trim().is_empty() {
                        out.push(stmt);
                    }
                    start = i + 1;
                    i += 1;
                }
                _ => {
                    i += 1;
                }
            },
            State::SingleQuote => {
                if bytes[i] == b'\'' {
                    // SQLite uses doubled single-quotes to escape a quote inside strings.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        state = State::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::DoubleQuote => {
                if bytes[i] == b'"' {
                    // Doubled double-quotes escape a quote inside an identifier.
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        state = State::Normal;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            State::LineComment => {
                if bytes[i] == b'\n' {
                    state = State::Normal;
                }
                i += 1;
            }
            State::BlockComment => {
                if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
                    state = State::Normal;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }
    }

    let tail = &sql[start..];
    if !tail.trim().is_empty() {
        out.push(tail);
    }

    out
}

pub(crate) async fn d1_execute_unprepared(
    conn: &sqlx_d1::D1Connection,
    sql: &str,
) -> Result<D1QueryResult, DbErr> {
    let stmts = split_unprepared_sql(sql);

    // Fast-path: most calls are single-statement.
    if stmts.len() <= 1 {
        return sqlx_d1::query(sql)
            .execute(conn)
            .await
            .map_err(d1_sqlx_error_to_exec_err);
    }

    let mut last: Option<D1QueryResult> = None;
    for stmt in stmts {
        last = Some(
            sqlx_d1::query(stmt)
                .execute(conn)
                .await
                .map_err(d1_sqlx_error_to_exec_err)?,
        );
    }

    last.ok_or_else(|| DbErr::Exec(crate::RuntimeErr::Internal("empty SQL".to_owned())))
}

/// Defines the Cloudflare D1 connector (via `sqlx-d1`).
///
/// Note: D1 does not use a traditional connection pool.
#[derive(Debug)]
pub struct SqlxD1Connector;

/// Defines a sqlx-d1 connection wrapper.
///
/// D1 runs in a single-threaded Wasm isolate. The underlying `sqlx_d1::D1Connection` is `Clone`.
#[derive(Clone)]
pub struct SqlxD1Connection {
    pub(crate) conn: sqlx_d1::D1Connection,
    metric_callback: Option<crate::metric::Callback>,
}

impl std::fmt::Debug for SqlxD1Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SqlxD1Connection {{ .. }}")
    }
}

impl From<sqlx_d1::D1Connection> for SqlxD1Connection {
    fn from(conn: sqlx_d1::D1Connection) -> Self {
        Self {
            conn,
            metric_callback: None,
        }
    }
}

impl From<sqlx_d1::D1Connection> for DatabaseConnection {
    fn from(conn: sqlx_d1::D1Connection) -> Self {
        DatabaseConnectionType::SqlxD1Connection(conn.into()).into()
    }
}

impl SqlxD1Connector {
    /// Create a D1-backed SeaORM connection from a Workers D1 binding.
    ///
    /// This does not require any async runtime (Tokio/async-std).
    pub fn connect(d1: worker::D1Database) -> Result<DatabaseConnection, DbErr> {
        let conn = sqlx_d1::D1Connection::new(d1);
        Ok(DatabaseConnectionType::SqlxD1Connection(conn.into()).into())
    }

    /// (Advanced) Create a D1-backed SeaORM connection from existing `sqlx_d1::D1Connection`.
    pub fn from_sqlx_d1_connection(conn: sqlx_d1::D1Connection) -> DatabaseConnection {
        DatabaseConnectionType::SqlxD1Connection(conn.into()).into()
    }

    /// (Advanced) Create a D1-backed SeaORM connection from `ConnectOptions`.
    ///
    /// This is primarily useful for non-Wasm local development against Miniflare's D1 SQLite
    /// emulator (see `sqlx-d1` docs). On `wasm32`, `sqlx_d1::D1ConnectOptions` does not support
    /// URL-based construction.
    #[instrument(level = "trace")]
    pub async fn connect_with_options(options: ConnectOptions) -> Result<DatabaseConnection, DbErr> {
        let mut options = options;

        if options.get_max_connections().is_none() {
            options.max_connections(1);
        }

        let after_conn = options.after_connect.clone();

        let d1_opts = options
            .url
            .parse::<sqlx_d1::D1ConnectOptions>()
            .map_err(d1_sqlx_error_to_conn_err)?;

        let conn = d1_opts
            .connect()
            .await
            .map_err(|e| DbErr::Conn(crate::RuntimeErr::Internal(e.to_string())))?;

        let conn: DatabaseConnection =
            DatabaseConnectionType::SqlxD1Connection(SqlxD1Connection::from(conn)).into();

        if let Some(cb) = after_conn {
            cb(conn.clone()).await?;
        }

        Ok(conn)
    }
}

impl SqlxD1Connection {
    pub(crate) fn set_metric_callback<F>(&mut self, callback: F)
    where
        F: Fn(&crate::metric::Info<'_>) + Send + Sync + 'static,
    {
        self.metric_callback = Some(Arc::new(callback));
    }

    /// Execute a prepared [Statement] on a D1 backend.
    #[instrument(level = "trace")]
    pub async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        debug_print!("{}", stmt);

        let query = sqlx_query(&stmt)?;
        crate::metric::metric!(self.metric_callback, &stmt, {
            match query.execute(&self.conn).await {
                Ok(res) => Ok(ExecResult {
                    result: ExecResultHolder::SqlxD1(res),
                }),
                Err(err) => Err(d1_sqlx_error_to_exec_err(err)),
            }
        })
    }

    /// Execute an unprepared SQL statement on a D1 backend.
    #[instrument(level = "trace")]
    pub async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        debug_print!("{}", sql);

        let res = d1_execute_unprepared(&self.conn, sql).await?;
        Ok(ExecResult {
            result: ExecResultHolder::SqlxD1(res),
        })
    }

    /// Get one result from a SQL query. Returns [Option::None] if no match was found.
    #[instrument(level = "trace")]
    pub async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        debug_print!("{}", stmt);

        let query = sqlx_query(&stmt)?;
        crate::metric::metric!(self.metric_callback, &stmt, {
            match query.fetch_optional(&self.conn).await {
                Ok(row) => Ok(row.map(|row| QueryResult {
                    row: QueryResultRow::SqlxD1(row),
                })),
                Err(err) => Err(d1_sqlx_error_to_query_err(err)),
            }
        })
    }

    /// Get the results of a query returning them as a Vec<[QueryResult]>.
    #[instrument(level = "trace")]
    pub async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        debug_print!("{}", stmt);

        let query = sqlx_query(&stmt)?;
        crate::metric::metric!(self.metric_callback, &stmt, {
            match query.fetch_all(&self.conn).await {
                Ok(rows) => Ok(rows
                    .into_iter()
                    .map(|row| QueryResult {
                        row: QueryResultRow::SqlxD1(row),
                    })
                    .collect()),
                Err(err) => Err(d1_sqlx_error_to_query_err(err)),
            }
        })
    }

    /// Stream results.
    ///
    /// D1 does not support server-side streaming. This returns an error.
    #[instrument(level = "trace")]
    pub async fn stream(&self, _stmt: Statement) -> Result<QueryStream, DbErr> {
        Err(DbErr::BackendNotSupported {
            db: "D1",
            ctx: "QueryStream",
        })
    }

    /// Begin a transaction.
    ///
    /// D1's transaction support is limited; `sqlx-d1` implements transactions as no-ops.
    #[instrument(level = "trace")]
    pub async fn begin(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<DatabaseTransaction, DbErr> {
        if isolation_level.is_some() || access_mode.is_some() {
            warn!("D1 does not support configuring transactions; settings will be ignored");
        }

        DatabaseTransaction::new_d1(self.conn.clone(), self.metric_callback.clone()).await
    }

    /// Run a transactional callback.
    ///
    /// Note: this does not provide atomicity on D1; it exists for API compatibility.
    #[instrument(level = "trace", skip(callback))]
    pub async fn transaction<F, T, E>(
        &self,
        callback: F,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<T, TransactionError<E>>
    where
        F: for<'b> FnOnce(
                &'b DatabaseTransaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'b>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        if isolation_level.is_some() || access_mode.is_some() {
            warn!("D1 does not support configuring transactions; settings will be ignored");
        }

        let transaction = DatabaseTransaction::new_d1(self.conn.clone(), self.metric_callback.clone())
            .await
            .map_err(TransactionError::Connection)?;
        transaction.run(callback).await
    }

    /// Checks if the connection is still valid.
    pub async fn ping(&self) -> Result<(), DbErr> {
        // D1 has no persistent TCP connection; treat a simple no-op query as a ping.
        self.execute_unprepared("SELECT 1")
            .await
            .map(|_| ())
    }

    /// Explicitly close the D1 connection.
    pub async fn close(self) -> Result<(), DbErr> {
        self.close_by_ref().await
    }

    /// Explicitly close the D1 connection.
    pub async fn close_by_ref(&self) -> Result<(), DbErr> {
        // D1Connection::close is a no-op.
        Ok(())
    }
}

type D1Arguments = <sqlx_d1::D1 as sqlx::Database>::Arguments<'static>;

pub(crate) fn sqlx_query(
    stmt: &Statement,
) -> Result<::sqlx_d1::query::Query<'_, ::sqlx_d1::D1, D1Arguments>, DbErr> {
    let values = stmt
        .values
        .as_ref()
        .map_or(Values(Vec::new()), |values| values.clone());

    let args: D1Arguments = SqlxValues(values).into_arguments();
    Ok(::sqlx_d1::query_with(&stmt.sql, args))
}

impl crate::DatabaseTransaction {
    pub(crate) async fn new_d1(
        inner: sqlx_d1::D1Connection,
        metric_callback: Option<crate::metric::Callback>,
    ) -> Result<crate::DatabaseTransaction, DbErr> {
        Self::begin(
            Arc::new(Mutex::new(crate::InnerConnection::D1(inner))),
            crate::DbBackend::Sqlite,
            metric_callback,
            None,
            None,
        )
        .await
    }
}
