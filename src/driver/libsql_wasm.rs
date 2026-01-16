use std::sync::Arc;

use tracing::{debug, instrument};

use crate::{
    AccessMode, ColIdx, ConnectOptions, DatabaseConnection, DatabaseConnectionType,
    DatabaseTransaction, InnerConnection, IsolationLevel, QueryStream, Statement, TransactionError,
    error::*, executor::*,
};

use libsql::params::Params as LibsqlParams;
use libsql::wasm::{CloudflareSender, Connection as LibsqlConnection, Rows as LibsqlRows, Transaction as LibsqlTransaction};
use url::Url;

pub use libsql::{Error as LibsqlError, Value as LibsqlValue};

type LibsqlWasmConnection = LibsqlConnection<CloudflareSender>;
type LibsqlWasmTransaction = LibsqlTransaction<CloudflareSender>;

/// A helper class to connect to libsql
#[derive(Debug)]
pub struct LibsqlConnector;

/// Defines a libsql connection sharable across threads.
#[derive(Clone)]
pub struct LibsqlSharedConnection {
    conn: LibsqlWasmConnection,
    metric_callback: Option<crate::metric::Callback>,
}

pub(crate) struct LibsqlInnerConnection {
    conn: LibsqlWasmConnection,
    tx: Option<LibsqlWasmTransaction>,
    transaction_depth: u32,
}

#[derive(Debug)]
pub(crate) struct LibsqlExecResult {
    pub(crate) rows_affected: u64,
    pub(crate) last_insert_rowid: i64,
}

#[derive(Debug)]
pub(crate) struct OwnedRow {
    pub(crate) columns: Vec<Arc<str>>,
    pub(crate) values: Vec<LibsqlValue>,
}

/// Decode a Rust type from a libsql value.
///
/// This is used by SeaORM's `TryGetable` implementations for libsql.
pub(crate) trait FromLibsqlValue: Sized {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError>;
}

impl<T: FromLibsqlValue> FromLibsqlValue for Option<T> {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Null => Ok(None),
            other => Ok(Some(T::try_from_libsql_value(other)?)),
        }
    }
}

macro_rules! int_from_i64 {
    ($t:ty, $into:literal) => {
        impl FromLibsqlValue for $t {
            fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
                let i = match value {
                    LibsqlValue::Integer(i) => *i,
                    LibsqlValue::Real(f) => *f as i64,
                    LibsqlValue::Text(s) => s.parse::<i64>().map_err(|e| {
                        TryGetError::DbErr(DbErr::TryIntoErr {
                            from: "String",
                            into: $into,
                            source: Arc::new(e),
                        })
                    })?,
                    LibsqlValue::Blob(_) => {
                        return Err(type_err(format!("cannot decode {} from BLOB", $into)).into())
                    }
                    LibsqlValue::Null => return Err(TryGetError::Null("<value>".into())),
                };

                i.try_into().map_err(|e| {
                    TryGetError::DbErr(DbErr::TryIntoErr {
                        from: "i64",
                        into: $into,
                        source: Arc::new(e),
                    })
                })
            }
        }
    };
}

int_from_i64!(i8, "i8");
int_from_i64!(i16, "i16");
int_from_i64!(i32, "i32");
int_from_i64!(i64, "i64");
int_from_i64!(u8, "u8");
int_from_i64!(u16, "u16");
int_from_i64!(u32, "u32");

impl FromLibsqlValue for bool {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Integer(i) => Ok(*i != 0),
            LibsqlValue::Real(f) => Ok(*f != 0.0),
            LibsqlValue::Text(s) => match s.as_str() {
                "0" => Ok(false),
                "1" => Ok(true),
                "false" | "FALSE" => Ok(false),
                "true" | "TRUE" => Ok(true),
                _ => Err(type_err(format!("cannot decode bool from `{s}`")).into()),
            },
            LibsqlValue::Blob(_) => Err(type_err("cannot decode bool from BLOB").into()),
            LibsqlValue::Null => Err(TryGetError::Null("<value>".into())),
        }
    }
}

impl FromLibsqlValue for f32 {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        let f = <f64 as FromLibsqlValue>::try_from_libsql_value(value)?;
        Ok(f as f32)
    }
}

impl FromLibsqlValue for f64 {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Real(f) => Ok(*f),
            LibsqlValue::Integer(i) => Ok(*i as f64),
            LibsqlValue::Text(s) => s.parse::<f64>().map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "f64",
                    source: Arc::new(e),
                })
            }),
            LibsqlValue::Blob(_) => Err(type_err("cannot decode f64 from BLOB").into()),
            LibsqlValue::Null => Err(TryGetError::Null("<value>".into())),
        }
    }
}

impl FromLibsqlValue for Vec<u8> {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Blob(b) => Ok(b.clone()),
            LibsqlValue::Text(s) => Ok(s.as_bytes().to_vec()),
            LibsqlValue::Integer(_) | LibsqlValue::Real(_) => {
                Err(type_err("cannot decode Vec<u8> from numeric").into())
            }
            LibsqlValue::Null => Err(TryGetError::Null("<value>".into())),
        }
    }
}

impl FromLibsqlValue for String {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Text(s) => Ok(s.clone()),
            LibsqlValue::Integer(i) => Ok(i.to_string()),
            LibsqlValue::Real(f) => Ok(f.to_string()),
            LibsqlValue::Blob(_) => Err(type_err("cannot decode String from BLOB").into()),
            LibsqlValue::Null => Err(TryGetError::Null("<value>".into())),
        }
    }
}

#[cfg(feature = "with-json")]
impl FromLibsqlValue for serde_json::Value {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        match value {
            LibsqlValue::Text(s) => serde_json::from_str(s).map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "serde_json::Value",
                    source: Arc::new(e),
                })
            }),
            LibsqlValue::Null => Err(TryGetError::Null("<value>".into())),
            _ => Err(type_err("cannot decode JSON from non-text value").into()),
        }
    }
}

#[cfg(feature = "with-uuid")]
impl FromLibsqlValue for uuid::Uuid {
    fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
        let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
        uuid::Uuid::parse_str(&s).map_err(|e| {
            TryGetError::DbErr(DbErr::TryIntoErr {
                from: "String",
                into: "uuid::Uuid",
                source: Arc::new(e),
            })
        })
    }
}

#[cfg(feature = "with-chrono")]
mod chrono_decode {
    use super::*;

    impl FromLibsqlValue for chrono::NaiveDate {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            s.parse::<chrono::NaiveDate>().map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "chrono::NaiveDate",
                    source: Arc::new(e),
                })
            })
        }
    }

    impl FromLibsqlValue for chrono::NaiveTime {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            s.parse::<chrono::NaiveTime>().map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "chrono::NaiveTime",
                    source: Arc::new(e),
                })
            })
        }
    }

    impl FromLibsqlValue for chrono::NaiveDateTime {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            s.parse::<chrono::NaiveDateTime>().map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "chrono::NaiveDateTime",
                    source: Arc::new(e),
                })
            })
        }
    }

    impl FromLibsqlValue for chrono::DateTime<chrono::Utc> {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                return Ok(dt.with_timezone(&chrono::Utc));
            }
            let ndt = s.parse::<chrono::NaiveDateTime>().map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "chrono::DateTime<Utc>",
                    source: Arc::new(e),
                })
            })?;
            Ok(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc))
        }
    }

    impl FromLibsqlValue for chrono::DateTime<chrono::FixedOffset> {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                return Ok(dt);
            }
            Err(type_err("cannot decode chrono::DateTime<FixedOffset> from non-rfc3339 text")
                .into())
        }
    }

    impl FromLibsqlValue for chrono::DateTime<chrono::Local> {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let utc = <chrono::DateTime<chrono::Utc> as FromLibsqlValue>::try_from_libsql_value(
                value,
            )?;
            Ok(utc.with_timezone(&chrono::Local))
        }
    }
}

#[cfg(feature = "with-time")]
mod time_decode {
    use super::*;

    impl FromLibsqlValue for time::Date {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            time::Date::parse(&s, sea_query::value::time_format::FORMAT_DATE).map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "time::Date",
                    source: Arc::new(e),
                })
            })
        }
    }

    impl FromLibsqlValue for time::Time {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            time::Time::parse(&s, sea_query::value::time_format::FORMAT_TIME).map_err(|e| {
                TryGetError::DbErr(DbErr::TryIntoErr {
                    from: "String",
                    into: "time::Time",
                    source: Arc::new(e),
                })
            })
        }
    }

    impl FromLibsqlValue for time::PrimitiveDateTime {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            time::PrimitiveDateTime::parse(&s, sea_query::value::time_format::FORMAT_DATETIME)
                .map_err(|e| {
                    TryGetError::DbErr(DbErr::TryIntoErr {
                        from: "String",
                        into: "time::PrimitiveDateTime",
                        source: Arc::new(e),
                    })
                })
        }
    }

    impl FromLibsqlValue for time::OffsetDateTime {
        fn try_from_libsql_value(value: &LibsqlValue) -> Result<Self, TryGetError> {
            let s = <String as FromLibsqlValue>::try_from_libsql_value(value)?;
            time::OffsetDateTime::parse(&s, sea_query::value::time_format::FORMAT_DATETIME_TZ)
                .map_err(|e| {
                    TryGetError::DbErr(DbErr::TryIntoErr {
                        from: "String",
                        into: "time::OffsetDateTime",
                        source: Arc::new(e),
                    })
                })
        }
    }
}

impl std::fmt::Debug for LibsqlSharedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LibsqlSharedConnection")
    }
}

impl std::fmt::Debug for LibsqlInnerConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LibsqlInnerConnection")
    }
}

impl From<LibsqlWasmConnection> for LibsqlSharedConnection {
    fn from(conn: LibsqlWasmConnection) -> Self {
        LibsqlSharedConnection {
            conn,
            metric_callback: None,
        }
    }
}

impl From<LibsqlSharedConnection> for DatabaseConnection {
    fn from(conn: LibsqlSharedConnection) -> Self {
        DatabaseConnectionType::LibsqlSharedConnection(conn).into()
    }
}

impl LibsqlConnector {
    /// Add configuration options for the libsql database.
    #[instrument(level = "trace")]
    pub async fn connect(options: ConnectOptions) -> Result<DatabaseConnection, DbErr> {
        let url = Url::parse(&options.url).map_err(|e| conn_err(e.to_string()))?;
        if url.scheme() != "libsql" {
            return Err(conn_err(format!(
                "The connection string '{}' has no supporting driver.",
                options.url
            )));
        }

        let auth_token = resolve_libsql_auth_token(&options, &url)
            .ok_or_else(|| conn_err("libsql auth token is required for wasm connections"))?;

        let after_conn = options.after_connect;

        let host = url.host_str().unwrap_or_default();
        if host.is_empty() {
            return Err(DbErr::BackendNotSupported {
                db: "libsql",
                ctx: "local libsql URLs are not supported on wasm",
            });
        }

        let url = strip_auth_from_url(url).to_string();
        let conn = LibsqlWasmConnection::open_cloudflare_worker(url, auth_token);
        let conn: DatabaseConnection = LibsqlSharedConnection::from(conn).into();

        if let Some(cb) = after_conn {
            cb(conn.clone()).await?;
        }

        Ok(conn)
    }
}

impl LibsqlSharedConnection {
    /// Set a callback for collecting statement-level metrics.
    pub fn set_metric_callback<F>(&mut self, callback: F)
    where
        F: Fn(&crate::metric::Info<'_>) + Send + Sync + 'static,
    {
        self.metric_callback = Some(Arc::new(callback));
    }

    fn loan(&self) -> LibsqlInnerConnection {
        LibsqlInnerConnection {
            conn: self.conn.clone(),
            tx: None,
            transaction_depth: 0,
        }
    }

    #[instrument(level = "trace")]
    /// Execute a [Statement] on a libsql backend.
    pub async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        debug!("{}", stmt);

        let mut conn = self.loan();
        conn.execute(stmt, &self.metric_callback).await
    }

    #[instrument(level = "trace")]
    /// Execute an unprepared SQL statement.
    pub async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        debug!("{}", sql);

        let mut conn = self.loan();
        conn.execute_unprepared(sql, &self.metric_callback).await
    }

    #[instrument(level = "trace")]
    /// Execute a [Statement] and return at most one row.
    pub async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        debug!("{}", stmt);

        let mut conn = self.loan();
        conn.query_one(stmt, &self.metric_callback).await
    }

    #[instrument(level = "trace")]
    /// Execute a [Statement] and return all rows.
    pub async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        debug!("{}", stmt);

        let mut conn = self.loan();
        conn.query_all(stmt, &self.metric_callback).await
    }

    /// Stream the results of executing a SQL query.
    #[instrument(level = "trace")]
    pub fn stream(&self, stmt: Statement) -> Result<QueryStream, DbErr> {
        debug!("{}", stmt);

        Ok(QueryStream::build(
            stmt,
            InnerConnection::Libsql(self.loan()),
            self.metric_callback.clone(),
        ))
    }

    /// Bundle a set of SQL statements that execute together.
    #[instrument(level = "trace")]
    pub async fn begin(
        &self,
        isolation_level: Option<IsolationLevel>,
        access_mode: Option<AccessMode>,
    ) -> Result<DatabaseTransaction, DbErr> {
        let conn = self.loan();
        DatabaseTransaction::begin(
            Arc::new(futures_util::lock::Mutex::new(InnerConnection::Libsql(conn))),
            crate::DbBackend::Sqlite,
            self.metric_callback.clone(),
            isolation_level,
            access_mode,
        )
        .await
    }

    /// Create a libsql transaction.
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
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send + 'b>>
            + Send,
        T: Send,
        E: std::fmt::Display + std::fmt::Debug + Send,
    {
        self.begin(isolation_level, access_mode)
            .await
            .map_err(TransactionError::Connection)?
            .run(callback)
            .await
    }

    /// Checks if a connection to the database is still valid.
    pub async fn ping(&self) -> Result<(), DbErr> {
        let mut conn = self.loan();
        conn.ping().await
    }

    /// Explicitly close the connection.
    pub async fn close_by_ref(&self) -> Result<(), DbErr> {
        Ok(())
    }
}

impl LibsqlInnerConnection {
    fn active_conn(&self) -> LibsqlActive<'_> {
        if let Some(tx) = &self.tx {
            LibsqlActive::Tx(tx)
        } else {
            LibsqlActive::Conn(&self.conn)
        }
    }

    pub(crate) async fn query_rows(
        &self,
        stmt: &Statement,
    ) -> Result<(LibsqlRows, Vec<Arc<str>>), DbErr> {
        let values = sql_values(stmt)?;
        let params = LibsqlParams::Positional(values);
        let rows = match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.query(&stmt.sql, params).await.map_err(query_err)?,
            LibsqlActive::Conn(conn) => conn.query(&stmt.sql, params).await.map_err(query_err)?,
        };
        let columns = column_names(&rows);
        Ok((rows, columns))
    }

    #[instrument(level = "trace", skip(metric_callback))]
    pub async fn execute(
        &mut self,
        stmt: Statement,
        metric_callback: &Option<crate::metric::Callback>,
    ) -> Result<ExecResult, DbErr> {
        debug!("{}", stmt);

        let values = sql_values(&stmt)?;
        let params = LibsqlParams::Positional(values);
        let rows_affected = match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.execute(&stmt.sql, params).await.map_err(exec_err)?,
            LibsqlActive::Conn(conn) => conn.execute(&stmt.sql, params).await.map_err(exec_err)?,
        };

        crate::metric::metric!(metric_callback, &stmt, {
            Ok(LibsqlExecResult {
                rows_affected,
                last_insert_rowid: 0,
            }
            .into())
        })
    }

    #[instrument(level = "trace", skip(_metric_callback))]
    pub async fn execute_unprepared(
        &mut self,
        sql: &str,
        _metric_callback: &Option<crate::metric::Callback>,
    ) -> Result<ExecResult, DbErr> {
        debug!("{}", sql);

        match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.execute_batch(sql).await.map_err(exec_err)?,
            LibsqlActive::Conn(conn) => conn.execute_batch(sql).await.map_err(exec_err)?,
        };

        Ok(LibsqlExecResult {
            rows_affected: 0,
            last_insert_rowid: 0,
        }
        .into())
    }

    #[instrument(level = "trace", skip(metric_callback))]
    pub async fn query_one(
        &mut self,
        stmt: Statement,
        metric_callback: &Option<crate::metric::Callback>,
    ) -> Result<Option<QueryResult>, DbErr> {
        debug!("{}", stmt);

        let values = sql_values(&stmt)?;
        let params = LibsqlParams::Positional(values);
        let mut rows = match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.query(&stmt.sql, params).await.map_err(query_err)?,
            LibsqlActive::Conn(conn) => conn.query(&stmt.sql, params).await.map_err(query_err)?,
        };
        let cols = column_names(&rows);

        crate::metric::metric!(metric_callback, &stmt, {
            if let Some(row) = rows.next().await.map_err(query_err)? {
                let owned = OwnedRow::from_row(cols, &row).map_err(query_err)?;
                Ok(Some(owned.into()))
            } else {
                Ok(None)
            }
        })
    }

    #[instrument(level = "trace", skip(metric_callback))]
    pub async fn query_all(
        &mut self,
        stmt: Statement,
        metric_callback: &Option<crate::metric::Callback>,
    ) -> Result<Vec<QueryResult>, DbErr> {
        debug!("{}", stmt);

        let values = sql_values(&stmt)?;
        let params = LibsqlParams::Positional(values);
        let mut rows = match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.query(&stmt.sql, params).await.map_err(query_err)?,
            LibsqlActive::Conn(conn) => conn.query(&stmt.sql, params).await.map_err(query_err)?,
        };
        let cols = column_names(&rows);

        crate::metric::metric!(metric_callback, &stmt, {
            let mut out = Vec::new();
            while let Some(row) = rows.next().await.map_err(query_err)? {
                let owned = OwnedRow::from_row(cols.clone(), &row).map_err(query_err)?;
                out.push(owned.into());
            }
            Ok(out)
        })
    }

    pub async fn ping(&mut self) -> Result<(), DbErr> {
        let mut rows = match self.active_conn() {
            LibsqlActive::Tx(tx) => tx.query("SELECT 1", ()).await.map_err(query_err)?,
            LibsqlActive::Conn(conn) => conn.query("SELECT 1", ()).await.map_err(query_err)?,
        };
        let _ = rows.next().await.map_err(query_err)?;
        Ok(())
    }

    #[instrument(level = "trace")]
    pub(crate) async fn begin(&mut self) -> Result<(), DbErr> {
        if self.transaction_depth > 0 {
            return Err(DbErr::BackendNotSupported {
                db: "libsql",
                ctx: "nested transactions are not supported on wasm",
            });
        }

        let tx = self
            .conn
            .transaction(libsql::TransactionBehavior::Deferred)
            .await
            .map_err(query_err)?;
        self.tx = Some(tx);
        self.transaction_depth = 1;
        Ok(())
    }

    #[instrument(level = "trace")]
    pub(crate) async fn commit(&mut self) -> Result<(), DbErr> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        let mut tx = self.tx.take().ok_or_else(|| conn_err("missing transaction"))?;
        tx.commit().await.map_err(query_err)?;
        self.transaction_depth = 0;
        Ok(())
    }

    #[instrument(level = "trace")]
    pub(crate) async fn rollback(&mut self) -> Result<(), DbErr> {
        if self.transaction_depth == 0 {
            return Ok(());
        }

        let mut tx = self.tx.take().ok_or_else(|| conn_err("missing transaction"))?;
        tx.rollback().await.map_err(query_err)?;
        self.transaction_depth = 0;
        Ok(())
    }

    #[instrument(level = "trace")]
    pub(crate) fn start_rollback(&mut self) -> Result<(), DbErr> {
        if self.transaction_depth > 0 {
            let _ = self.tx.take();
            self.transaction_depth = 0;
        }
        Ok(())
    }
}

enum LibsqlActive<'a> {
    Conn(&'a LibsqlWasmConnection),
    Tx(&'a LibsqlWasmTransaction),
}

impl OwnedRow {
    pub(crate) fn from_row(
        columns: Vec<Arc<str>>,
        row: &libsql::Row,
    ) -> libsql::Result<OwnedRow> {
        let mut values = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            values.push(row.get_value(i as i32)?);
        }
        Ok(OwnedRow { columns, values })
    }

    pub fn try_get<T: FromLibsqlValue, I: ColIdx>(&self, idx: I) -> Result<T, TryGetError> {
        let value = if let Some(idx) = idx.as_usize() {
            self.values
                .get(*idx)
                .ok_or_else(|| TryGetError::Null(format!("column index {idx} out of range")))?
        } else if let Some(name) = idx.as_str() {
            if let Some(pos) = self.columns.iter().position(|c| c.as_ref() == name) {
                self.values
                    .get(pos)
                    .ok_or_else(|| TryGetError::Null(format!("column `{name}` out of range")))?
            } else {
                return Err(TryGetError::Null(format!(
                    "column `{name}` does not exist in row"
                )));
            }
        } else {
            unreachable!("ColIdx must be either usize or str")
        };

        T::try_from_libsql_value(value)
    }
}

fn column_names(rows: &LibsqlRows) -> Vec<Arc<str>> {
    let mut cols = Vec::new();
    let count = rows.column_count();
    for i in 0..count {
        if let Some(name) = rows.column_name(i) {
            cols.push(Arc::from(name));
        } else {
            cols.push(Arc::from(""));
        }
    }
    cols
}

fn sql_values(stmt: &Statement) -> Result<Vec<LibsqlValue>, DbErr> {
    let Some(values) = &stmt.values else {
        return Ok(Vec::new());
    };

    let mut out = Vec::with_capacity(values.0.len());

    for v in values.0.iter() {
        out.push(sea_value_to_libsql_value(v)?);
    }

    Ok(out)
}

fn sea_value_to_libsql_value(v: &crate::Value) -> Result<LibsqlValue, DbErr> {
    use crate::Value;

    #[allow(unreachable_patterns)]
    Ok(match v {
        Value::Bool(v) => LibsqlValue::from(*v),
        Value::TinyInt(v) => LibsqlValue::from(*v),
        Value::SmallInt(v) => LibsqlValue::from(*v),
        Value::Int(v) => LibsqlValue::from(*v),
        Value::BigInt(v) => LibsqlValue::from(*v),
        Value::TinyUnsigned(v) => LibsqlValue::from(v.map(|v| v as i64)),
        Value::SmallUnsigned(v) => LibsqlValue::from(v.map(|v| v as i64)),
        Value::Unsigned(v) => LibsqlValue::from(v.map(|v| v as i64)),
        Value::BigUnsigned(v) => match v {
            Some(v) => {
                let i: i64 = (*v).try_into().map_err(|e| DbErr::TryIntoErr {
                    from: "u64",
                    into: "i64",
                    source: Arc::new(e),
                })?;
                LibsqlValue::Integer(i)
            }
            None => LibsqlValue::Null,
        },
        Value::Float(v) => LibsqlValue::from(v.map(|v| v as f64)),
        Value::Double(v) => LibsqlValue::from(*v),
        Value::String(v) => match v {
            Some(v) => LibsqlValue::Text(v.clone()),
            None => LibsqlValue::Null,
        },
        Value::Char(v) => match v {
            Some(v) => LibsqlValue::Text(v.to_string()),
            None => LibsqlValue::Null,
        },
        Value::Bytes(v) => match v {
            Some(v) => LibsqlValue::Blob(v.clone()),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-chrono")]
        Value::ChronoDate(_)
        | Value::ChronoTime(_)
        | Value::ChronoDateTime(_)
        | Value::ChronoDateTimeUtc(_)
        | Value::ChronoDateTimeLocal(_)
        | Value::ChronoDateTimeWithTimeZone(_) => match v.chrono_as_naive_utc_in_string() {
            Some(s) => LibsqlValue::Text(s),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-time")]
        Value::TimeDate(_)
        | Value::TimeTime(_)
        | Value::TimeDateTime(_)
        | Value::TimeDateTimeWithTimeZone(_) => match v.time_as_naive_utc_in_string() {
            Some(s) => LibsqlValue::Text(s),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-uuid")]
        Value::Uuid(v) => match v {
            Some(v) => LibsqlValue::Text(v.to_string()),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-json")]
        Value::Json(j) => match j {
            Some(v) if v.is_null() => LibsqlValue::Null,
            Some(v) => LibsqlValue::Text(v.to_string()),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-rust_decimal")]
        Value::Decimal(v) => match v {
            Some(v) => LibsqlValue::Text(v.to_string()),
            None => LibsqlValue::Null,
        },
        #[cfg(feature = "with-bigdecimal")]
        Value::BigDecimal(v) => match v {
            Some(v) => LibsqlValue::Text(v.to_string()),
            None => LibsqlValue::Null,
        },
        _ => {
            return Err(DbErr::BackendNotSupported {
                db: "Sqlite",
                ctx: "binding this value type with libsql",
            })
        }
    })
}

impl From<OwnedRow> for QueryResult {
    fn from(row: OwnedRow) -> QueryResult {
        QueryResult {
            row: crate::executor::QueryResultRow::Libsql(row),
        }
    }
}

impl From<LibsqlExecResult> for ExecResult {
    fn from(result: LibsqlExecResult) -> ExecResult {
        ExecResult {
            result: ExecResultHolder::Libsql(result),
        }
    }
}

fn resolve_libsql_auth_token(options: &ConnectOptions, url: &Url) -> Option<String> {
    options
        .libsql_auth_token
        .as_ref()
        .map(|t| t.to_string())
        .or_else(|| {
            url.query_pairs().find_map(|(k, v)| {
                if k == "authToken" || k == "auth_token" {
                    Some(v.to_string())
                } else {
                    None
                }
            })
        })
}

fn strip_auth_from_url(mut url: Url) -> Url {
    if url.query_pairs().any(|(k, _)| k == "authToken" || k == "auth_token") {
        url.set_query(None);
    }
    url
}

fn conn_err(err: impl ToString) -> DbErr {
    DbErr::Conn(RuntimeErr::Internal(err.to_string()))
}

fn exec_err(err: LibsqlError) -> DbErr {
    DbErr::Exec(RuntimeErr::Libsql(Arc::new(err)))
}

fn query_err(err: LibsqlError) -> DbErr {
    DbErr::Query(RuntimeErr::Libsql(Arc::new(err)))
}
