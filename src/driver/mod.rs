#[cfg(feature = "mock")]
mod mock;
#[cfg(feature = "proxy")]
mod proxy;
#[cfg(feature = "libsql")]
#[cfg_attr(target_arch = "wasm32", path = "libsql_wasm.rs")]
#[cfg_attr(not(target_arch = "wasm32"), path = "libsql.rs")]
pub(crate) mod libsql;
#[cfg(feature = "rusqlite")]
pub(crate) mod rusqlite;
#[cfg(any(feature = "sqlx-sqlite", feature = "rusqlite", feature = "libsql"))]
mod sqlite;
#[cfg(feature = "sqlx-dep")]
mod sqlx_common;
#[cfg(feature = "sqlx-mysql")]
pub(crate) mod sqlx_mysql;
#[cfg(feature = "sqlx-postgres")]
pub(crate) mod sqlx_postgres;
#[cfg(feature = "sqlx-sqlite")]
pub(crate) mod sqlx_sqlite;

#[cfg(feature = "mock")]
pub use mock::*;
#[cfg(feature = "proxy")]
pub use proxy::*;
#[cfg(feature = "libsql")]
pub use libsql::*;
#[cfg(feature = "sqlx-dep")]
pub(crate) use sqlx_common::*;
#[cfg(feature = "sqlx-mysql")]
pub use sqlx_mysql::*;
#[cfg(feature = "sqlx-postgres")]
pub use sqlx_postgres::*;
#[cfg(feature = "sqlx-sqlite")]
pub use sqlx_sqlite::*;
