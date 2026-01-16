
use std::{
	ops::Deref,
	sync::{Arc, Mutex, MutexGuard, TryLockError},
	time::{Duration, Instant},
};

use tracing::{debug, instrument};
use url::Url;

use crate::{
	AccessMode, ColIdx, ConnectOptions, DatabaseConnection, DatabaseConnectionType,
	DatabaseTransaction, InnerConnection, IsolationLevel, QueryStream, Statement, TransactionError,
	error::*, executor::*,
};

pub use libsql::{Connection as LibsqlConnection, Error as LibsqlError, Value as LibsqlValue};

/// A helper class to connect to libsql
#[derive(Debug)]
pub struct LibsqlConnector;

const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

/// Defines a libsql connection sharable across threads.
#[derive(Clone)]
pub struct LibsqlSharedConnection {
	conn: Arc<Mutex<State>>,
	acquire_timeout: Duration,
	metric_callback: Option<crate::metric::Callback>,
}

pub(crate) struct LibsqlInnerConnection {
	conn: LibsqlConnection,
	tx: Option<libsql::Transaction>,
	loan: Arc<Mutex<State>>,
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

#[derive(Debug, Default)]
enum State {
	Idle(LibsqlConnection),
	Loaned,
	#[default]
	Disconnected,
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
		write!(f, "LibsqlSharedConnection {{ conn: {:?} }}", self.conn)
	}
}

impl std::fmt::Debug for LibsqlInnerConnection {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "LibsqlInnerConnection")
	}
}

impl From<LibsqlConnection> for LibsqlSharedConnection {
	fn from(conn: LibsqlConnection) -> Self {
		LibsqlSharedConnection {
			conn: Arc::new(Mutex::new(State::Idle(conn))),
			acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
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
		let acquire_timeout = options.acquire_timeout.unwrap_or(DEFAULT_ACQUIRE_TIMEOUT);
		let after_conn = options.after_connect;

		let url = url::Url::parse(&options.url).map_err(|e| conn_err(e.to_string()))?;
		if url.scheme() != "libsql" {
			return Err(conn_err(format!(
				"The connection string '{}' has no supporting driver.",
				options.url
			)));
		}

		let auth_token = resolve_libsql_auth_token(&options, &url);
		let host = url.host_str().unwrap_or_default();
		let mut conn = if host.is_empty() {
			let raw_path = url.path();
			let mut path = raw_path;
			#[cfg(windows)]
			{
				if raw_path.starts_with('/') && raw_path.as_bytes().get(2) == Some(&b':') {
					path = &raw_path[1..];
				}
			}

			let path = if path.is_empty() || path == "/" {
				let tail = options
					.url
					.trim_start_matches("libsql://")
					.trim_start_matches("libsql:")
					.trim_start_matches('/');
				if tail.is_empty() {
					":memory:"
				} else {
					tail
				}
			} else {
				path
			};

			let db = libsql::Builder::new_local(std::path::Path::new(path))
				.build()
				.await
				.map_err(conn_err)?;
			let conn = db.connect().map_err(conn_err)?;

			LibsqlSharedConnection {
				conn: Arc::new(Mutex::new(State::Idle(conn))),
				acquire_timeout,
				metric_callback: None,
			}
		} else {
			let token = auth_token.ok_or_else(|| {
				conn_err("libsql auth token is required for remote connections")
			})?;
			let remote_url = strip_auth_from_url(url).to_string();
			let db = libsql::Builder::new_remote(remote_url, token)
				.build()
				.await
				.map_err(conn_err)?;
			let conn = db.connect().map_err(conn_err)?;

			LibsqlSharedConnection {
				conn: Arc::new(Mutex::new(State::Idle(conn))),
				acquire_timeout,
				metric_callback: None,
			}
		};

		#[cfg(feature = "sqlite-use-returning-for-3_35")]
		{
			let version = get_version_raw(&mut conn).await?;
			super::sqlite::ensure_returning_version(&version)?;
		}

		conn.execute_unprepared("PRAGMA foreign_keys = ON").await?;

		let conn: DatabaseConnection = conn.into();
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

	fn acquire(&self) -> Result<MutexGuard<'_, State>, DbErr> {
		let deadline = Instant::now() + self.acquire_timeout;
		loop {
			match self.conn.try_lock() {
				Ok(state) => match *state {
					State::Idle(_) => return Ok(state),
					State::Loaned => (),
					State::Disconnected => {
						return Err(DbErr::ConnectionAcquire(ConnAcquireErr::ConnectionClosed));
					}
				},
				Err(TryLockError::WouldBlock) => (),
				Err(TryLockError::Poisoned(_)) => {
					return Err(DbErr::ConnectionAcquire(ConnAcquireErr::ConnectionClosed));
				}
			}

			if Instant::now() >= deadline {
				return Err(DbErr::ConnectionAcquire(ConnAcquireErr::Timeout));
			}
			std::thread::yield_now();
		}
	}

	fn loan(&self) -> Result<LibsqlInnerConnection, DbErr> {
		let conn = {
			let mut guard = self.acquire()?;
			guard.loan()
		};

		Ok(LibsqlInnerConnection {
			conn,
			tx: None,
			loan: self.conn.clone(),
			transaction_depth: 0,
		})
	}

	#[instrument(level = "trace")]
	/// Execute a [Statement] on a libsql backend.
	pub async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
		debug!("{}", stmt);

		let mut conn = self.loan()?;
		conn.execute(stmt, &self.metric_callback).await
	}

	#[instrument(level = "trace")]
	/// Execute an unprepared SQL statement.
	pub async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
		debug!("{}", sql);

		let mut conn = self.loan()?;
		conn.execute_unprepared(sql, &self.metric_callback).await
	}

	#[instrument(level = "trace")]
	/// Execute a [Statement] and return at most one row.
	pub async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
		debug!("{}", stmt);

		let mut conn = self.loan()?;
		conn.query_one(stmt, &self.metric_callback).await
	}

	#[instrument(level = "trace")]
	/// Execute a [Statement] and return all rows.
	pub async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
		debug!("{}", stmt);

		let mut conn = self.loan()?;
		conn.query_all(stmt, &self.metric_callback).await
	}

	/// Stream the results of executing a SQL query.
	#[instrument(level = "trace")]
	pub fn stream(&self, stmt: Statement) -> Result<QueryStream, DbErr> {
		debug!("{}", stmt);

		Ok(QueryStream::build(
			stmt,
			InnerConnection::Libsql(self.loan()?),
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
		let conn = self.loan()?;
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
		let mut conn = self.loan()?;
		conn.ping().await
	}

	/// Explicitly close the connection.
	pub async fn close_by_ref(&self) -> Result<(), DbErr> {
		let mut state = self.acquire()?;
		*state = State::Disconnected;
		Ok(())
	}
}

impl LibsqlInnerConnection {
	fn active_conn(&self) -> &LibsqlConnection {
		if let Some(tx) = &self.tx {
			tx.deref()
		} else {
			&self.conn
		}
	}

	pub(crate) async fn query_rows(
		&self,
		stmt: &Statement,
	) -> Result<(libsql::Rows, Vec<Arc<str>>), DbErr> {
		let values = sql_values(stmt)?;
		let conn = self.active_conn();
		let rows = conn.query(&stmt.sql, values).await.map_err(query_err)?;
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
		let conn = self.active_conn();
		crate::metric::metric!(metric_callback, &stmt, {
			conn.execute(&stmt.sql, values)
				.await
				.map(|rows_affected| {
					LibsqlExecResult {
						rows_affected,
						last_insert_rowid: conn.last_insert_rowid(),
					}
					.into()
				})
				.map_err(exec_err)
		})
	}

	#[instrument(level = "trace", skip(_metric_callback))]
	pub async fn execute_unprepared(
		&mut self,
		sql: &str,
		_metric_callback: &Option<crate::metric::Callback>,
	) -> Result<ExecResult, DbErr> {
		debug!("{}", sql);

		let conn = self.active_conn();
		conn.execute_batch(sql).await.map_err(exec_err)?;

		Ok(LibsqlExecResult {
			rows_affected: conn.changes(),
			last_insert_rowid: conn.last_insert_rowid(),
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
		let conn = self.active_conn();

		crate::metric::metric!(metric_callback, &stmt, {
			let mut rows = conn.query(&stmt.sql, values).await.map_err(query_err)?;
			let columns = column_names(&rows);
			if let Some(row) = rows.next().await.map_err(query_err)? {
				let owned = OwnedRow::from_row(columns, &row).map_err(query_err)?;
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
		let conn = self.active_conn();

		crate::metric::metric!(metric_callback, &stmt, {
			let mut rows = conn.query(&stmt.sql, values).await.map_err(query_err)?;
			let columns = column_names(&rows);
			let mut out = Vec::new();
			while let Some(row) = rows.next().await.map_err(query_err)? {
				let owned = OwnedRow::from_row(columns.clone(), &row).map_err(query_err)?;
				out.push(owned.into());
			}
			Ok(out)
		})
	}

	pub async fn ping(&mut self) -> Result<(), DbErr> {
		let conn = self.active_conn();
		let mut rows = conn.query("SELECT 1", ()).await.map_err(query_err)?;
		let _ = rows.next().await.map_err(query_err)?;
		Ok(())
	}

	#[instrument(level = "trace")]
	pub(crate) async fn begin(&mut self) -> Result<(), DbErr> {
		if self.transaction_depth == 0 {
			let tx = self.conn.transaction().await.map_err(query_err)?;
			self.tx = Some(tx);
			self.transaction_depth = 1;
			return Ok(());
		}

		// Nested transaction = SAVEPOINT.
		let sql = format!("SAVEPOINT sp{}", self.transaction_depth);
		self.active_conn()
			.execute_batch(&sql)
			.await
			.map_err(query_err)?;
		self.transaction_depth += 1;
		Ok(())
	}

	#[instrument(level = "trace")]
	pub(crate) async fn commit(&mut self) -> Result<(), DbErr> {
		if self.transaction_depth == 0 {
			return Ok(());
		}

		if self.transaction_depth == 1 {
			let tx = self.tx.take().ok_or_else(|| conn_err("missing transaction"))?;
			tx.commit().await.map_err(query_err)?;
			self.transaction_depth = 0;
			return Ok(());
		}

		// Nested transaction = RELEASE SAVEPOINT.
		let sql = format!("RELEASE SAVEPOINT sp{}", self.transaction_depth - 1);
		self.active_conn()
			.execute_batch(&sql)
			.await
			.map_err(query_err)?;
		self.transaction_depth -= 1;
		Ok(())
	}

	#[instrument(level = "trace")]
	pub(crate) async fn rollback(&mut self) -> Result<(), DbErr> {
		if self.transaction_depth == 0 {
			return Ok(());
		}

		if self.transaction_depth == 1 {
			let tx = self.tx.take().ok_or_else(|| conn_err("missing transaction"))?;
			tx.rollback().await.map_err(query_err)?;
			self.transaction_depth = 0;
			return Ok(());
		}

		// Nested transaction = ROLLBACK TO SAVEPOINT + RELEASE.
		// NOTE: In SQLite, `ROLLBACK TO` does not remove the savepoint.
		let sp = self.transaction_depth - 1;
		let sql = format!("ROLLBACK TO sp{sp}; RELEASE SAVEPOINT sp{sp}");
		self.active_conn()
			.execute_batch(&sql)
			.await
			.map_err(query_err)?;
		self.transaction_depth -= 1;
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

impl Drop for LibsqlInnerConnection {
	fn drop(&mut self) {
		let _ = self.start_rollback();
		let mut loan = match self.loan.lock() {
			Ok(guard) => guard,
			Err(_) => return,
		};
		let replacement = self.conn.clone();
		let returned = std::mem::replace(&mut self.conn, replacement);
		loan.return_(returned);
	}
}

impl State {
	fn loan(&mut self) -> LibsqlConnection {
		let mut conn = State::Loaned;
		std::mem::swap(&mut conn, self);
		match conn {
			State::Idle(conn) => conn,
			_ => panic!("No connection"),
		}
	}

	fn return_(&mut self, conn: LibsqlConnection) {
		*self = State::Idle(conn);
	}
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
			if let Some(pos) = self.columns.iter().position(|c| c.deref() == name) {
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

fn column_names(rows: &libsql::Rows) -> Vec<Arc<str>> {
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

async fn get_version_raw(conn: &mut LibsqlSharedConnection) -> Result<String, DbErr> {
	let inner = conn.loan()?;
	let mut rows = inner
		.active_conn()
		.query("SELECT sqlite_version()", ())
		.await
		.map_err(query_err)?;
	if let Some(row) = rows.next().await.map_err(query_err)? {
		row.get::<String>(0).map_err(query_err)
	} else {
		Err(DbErr::Conn(RuntimeErr::Internal(
			"Error reading SQLite version".to_string(),
		)))
	}
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

