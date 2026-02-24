use ::libsql as libsql_core;
use pyo3::create_exception;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyModule, PyTuple};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use pyo3_asyncio::tokio::future_into_py;

const LEGACY_TRANSACTION_CONTROL: i32 = -1;

#[derive(Clone)]
enum ListOrTuple<'py> {
    List(Bound<'py, PyList>),
    Tuple(Bound<'py, PyTuple>),
}

struct ListOrTupleIterator<'py> {
    index: usize,
    inner: ListOrTuple<'py>,
}

fn to_py_err(error: libsql_core::errors::Error) -> PyErr {
    let msg = match error {
        libsql_core::Error::SqliteFailure(_, err) => err,
        _ => error.to_string(),
    };
    PyValueError::new_err(msg)
}

fn is_remote_path(path: &str) -> bool {
    path.starts_with("libsql://") || path.starts_with("http://") || path.starts_with("https://")
}

#[pyfunction]
#[pyo3(signature = (database, timeout=5.0, isolation_level="DEFERRED".to_string(), _check_same_thread=true, _uri=false, sync_url=None, sync_interval=None, offline=false, auth_token="", encryption_key=None, autocommit = LEGACY_TRANSACTION_CONTROL))]
fn connect(
    py: Python<'_>,
    database: String,
    timeout: f64,
    isolation_level: Option<String>,
    _check_same_thread: bool,
    _uri: bool,
    sync_url: Option<String>,
    sync_interval: Option<f64>,
    offline: bool,
    auth_token: String,
    encryption_key: Option<String>,
    autocommit: i32,
) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let ver = env!("CARGO_PKG_VERSION");
        let ver = format!("libsql-python-rpc-{ver}");
        let encryption_config = match encryption_key {
            Some(key) => {
                let cipher = libsql_core::Cipher::default();
                Some(libsql_core::EncryptionConfig::new(cipher, key.into()))
            }
            None => None,
        };

        let db = if is_remote_path(&database) {
            libsql_core::Database::open_remote_internal(database, &auth_token, ver)
                .map_err(to_py_err)?
        } else {
            match sync_url {
                Some(sync_url) => {
                    let sync_interval = sync_interval.map(|i| std::time::Duration::from_secs_f64(i));
                    let mut builder = libsql_core::Builder::new_synced_database(
                        database,
                        sync_url,
                        auth_token.to_string(),
                    );
                    if encryption_config.is_some() {
                        return Err(PyValueError::new_err(
                            "encryption is not supported for synced databases",
                        ));
                    }
                    if let Some(sync_interval) = sync_interval {
                        builder = builder.sync_interval(sync_interval);
                    }
                    builder = builder.remote_writes(!offline);
                    builder.build().await.map_err(to_py_err)?
                }
                None => {
                    let mut builder = libsql_core::Builder::new_local(database);
                    if let Some(config) = encryption_config {
                        builder = builder.encryption_config(config);
                    }
                    builder.build().await.map_err(to_py_err)?
                }
            }
        };

        let conn = db.connect().map_err(to_py_err)?;
        conn.busy_timeout(Duration::from_secs_f64(timeout)).map_err(to_py_err)?;
        
        let autocommit = if autocommit == LEGACY_TRANSACTION_CONTROL {
            isolation_level.is_none() as i32
        } else {
            if autocommit != 1 && autocommit != 0 {
                return Err(PyValueError::new_err(
                    "autocommit must be True, False, or sqlite3.LEGACY_TRANSACTION_CONTROL",
                ));
            }
            autocommit
        };

        Ok(Connection {
            db,
            conn: Arc::new(Mutex::new(Some(conn))),
            isolation_level,
            autocommit,
        })
    })
}

#[pyclass]
#[derive(Clone)]
pub struct Connection {
    db: libsql_core::Database,
    conn: Arc<Mutex<Option<libsql_core::Connection>>>,
    isolation_level: Option<String>,
    autocommit: i32,
}

#[pymethods]
impl Connection {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let mut conn = conn_arc.lock().await;
            if let Some(c) = conn.take() {
                drop(c);
            }
            Ok(())
        })
    }

    fn cursor(&self) -> PyResult<Cursor> {
        Ok(Cursor {
            arraysize: 1,
            conn: self.conn.clone(),
            stmt: Arc::new(Mutex::new(None)),
            rows: Arc::new(Mutex::new(None)),
            rowcount: Arc::new(Mutex::new(0)),
            isolation_level: self.isolation_level.clone(),
            autocommit: self.autocommit,
            done: Arc::new(Mutex::new(false)),
        })
    }

    fn sync<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let db = self.db.clone();
        future_into_py(py, async move {
            db.sync().await.map_err(to_py_err)?;
            Ok(())
        })
    }

    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let conn_guard = conn_arc.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                if !conn.is_autocommit() {
                    conn.execute("COMMIT", ()).await.map_err(to_py_err)?;
                }
            }
            Ok(())
        })
    }

    fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        future_into_py(py, async move {
            let conn_guard = conn_arc.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                if !conn.is_autocommit() {
                    conn.execute("ROLLBACK", ()).await.map_err(to_py_err)?;
                }
            }
            Ok(())
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<ListOrTuple<'py>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = cursor.clone().into_py(py);
        let params = extract_parameters(parameters)?;
        
        let conn_arc = cursor.conn.clone();
        let stmt_arc = cursor.stmt.clone();
        let rows_arc = cursor.rows.clone();
        let rowcount_arc = cursor.rowcount.clone();
        let autocommit = cursor.autocommit;
        let isolation_level = cursor.isolation_level.clone();

        future_into_py(py, async move {
            execute_async(
                conn_arc, stmt_arc, rows_arc, rowcount_arc,
                autocommit, isolation_level, sql, params
            ).await?;
            Python::with_gil(|py| Ok(cursor_py.into_bound(py)))
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        parameters: Option<&Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = cursor.clone().into_py(py);
        
        let mut params_list = vec![];
        if let Some(parameters) = parameters {
            for param in parameters.iter() {
                let lt = param.extract::<ListOrTuple>()?;
                params_list.push(extract_parameters(Some(lt))?);
            }
        }
        
        let conn_arc = cursor.conn.clone();
        let stmt_arc = cursor.stmt.clone();
        let rows_arc = cursor.rows.clone();
        let rowcount_arc = cursor.rowcount.clone();
        let autocommit = cursor.autocommit;
        let isolation_level = cursor.isolation_level.clone();

        future_into_py(py, async move {
            for params in params_list {
                execute_async(
                    conn_arc.clone(), stmt_arc.clone(), rows_arc.clone(), rowcount_arc.clone(),
                    autocommit, isolation_level.clone(), sql.clone(), params
                ).await?;
            }
            Python::with_gil(|py| Ok(cursor_py.into_bound(py)))
        })
    }

    fn executescript<'py>(&self, py: Python<'py>, script: String) -> PyResult<Bound<'py, PyAny>> {
        let cursor = self.cursor()?;
        let cursor_py = cursor.clone().into_py(py);
        let conn_arc = cursor.conn.clone();

        future_into_py(py, async move {
            let conn_guard = conn_arc.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                conn.execute_batch(&script).await.map_err(to_py_err)?;
            }
            Python::with_gil(|py| Ok(cursor_py.into_bound(py)))
        })
    }

    #[getter]
    fn isolation_level(&self) -> Option<String> {
        self.isolation_level.clone()
    }

    #[getter]
    fn in_transaction(&self) -> PyResult<bool> {
        Ok(self.autocommit == 0 || self.isolation_level.is_some())
    }

    #[getter]
    fn get_autocommit(&self) -> PyResult<i32> {
        Ok(self.autocommit)
    }

    #[setter]
    fn set_autocommit(&mut self, autocommit: i32) -> PyResult<()> {
        if autocommit != LEGACY_TRANSACTION_CONTROL && autocommit != 1 && autocommit != 0 {
            return Err(PyValueError::new_err(
                "autocommit must be True, False, or sqlite3.LEGACY_TRANSACTION_CONTROL",
            ));
        }
        self.autocommit = autocommit;
        Ok(())
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slf_py = slf.into_py(py);
        future_into_py(py, async move {
            Python::with_gil(|py| Ok(slf_py.into_bound(py)))
        })
    }

    #[pyo3(signature = (exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        let is_error = exc_type.is_some();
        future_into_py(py, async move {
            let conn_guard = conn_arc.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                if !conn.is_autocommit() {
                    if is_error {
                        let _ = conn.execute("ROLLBACK", ()).await;
                    } else {
                        let _ = conn.execute("COMMIT", ()).await;
                    }
                }
            }
            Ok(false)
        })
    }
}

#[pyclass]
#[derive(Clone)]
pub struct Cursor {
    #[pyo3(get, set)]
    arraysize: usize,
    conn: Arc<Mutex<Option<libsql_core::Connection>>>,
    stmt: Arc<Mutex<Option<libsql_core::Statement>>>,
    rows: Arc<Mutex<Option<libsql_core::Rows>>>,
    rowcount: Arc<Mutex<i64>>,
    done: Arc<Mutex<bool>>,
    isolation_level: Option<String>,
    autocommit: i32,
}

#[pymethods]
impl Cursor {
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let conn_arc = self.conn.clone();
        let stmt_arc = self.stmt.clone();
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            conn_arc.lock().await.take();
            stmt_arc.lock().await.take();
            rows_arc.lock().await.take();
            Ok(())
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn execute<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        sql: String,
        parameters: Option<ListOrTuple<'py>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = extract_parameters(parameters)?;
        let conn_arc = slf.conn.clone();
        let stmt_arc = slf.stmt.clone();
        let rows_arc = slf.rows.clone();
        let rowcount_arc = slf.rowcount.clone();
        let autocommit = slf.autocommit;
        let isolation_level = slf.isolation_level.clone();
        let slf_py = slf.into_py(py);
        
        future_into_py(py, async move {
            execute_async(
                conn_arc, stmt_arc, rows_arc, rowcount_arc,
                autocommit, isolation_level, sql, params
            ).await?;
            Python::with_gil(|py| Ok(slf_py.into_bound(py)))
        })
    }

    #[pyo3(signature = (sql, parameters=None))]
    fn executemany<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        sql: String,
        parameters: Option<&Bound<'py, PyList>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut params_list = vec![];
        if let Some(parameters) = parameters {
            for param in parameters.iter() {
                let lt = param.extract::<ListOrTuple>()?;
                params_list.push(extract_parameters(Some(lt))?);
            }
        }
        
        let conn_arc = slf.conn.clone();
        let stmt_arc = slf.stmt.clone();
        let rows_arc = slf.rows.clone();
        let rowcount_arc = slf.rowcount.clone();
        let autocommit = slf.autocommit;
        let isolation_level = slf.isolation_level.clone();
        let slf_py = slf.into_py(py);
        
        future_into_py(py, async move {
            for params in params_list {
                execute_async(
                    conn_arc.clone(), stmt_arc.clone(), rows_arc.clone(), rowcount_arc.clone(),
                    autocommit, isolation_level.clone(), sql.clone(), params
                ).await?;
            }
            Python::with_gil(|py| Ok(slf_py.into_bound(py)))
        })
    }

    fn executescript<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
        script: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let slf_py = slf.into_py(py);
        let conn_arc = slf.conn.clone();
        
        future_into_py(py, async move {
            let conn_guard = conn_arc.lock().await;
            if let Some(conn) = conn_guard.as_ref() {
                conn.execute_batch(&script).await.map_err(to_py_err)?;
            }
            Python::with_gil(|py| Ok(slf_py.into_bound(py)))
        })
    }

    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<Option<Bound<'_, PyTuple>>> {
        let stmt_guard = self.stmt.try_lock();
        if let Ok(stmt_ref) = stmt_guard {
            if let Some(stmt) = stmt_ref.as_ref() {
                let mut elements: Vec<Py<PyAny>> = vec![];
                for column in stmt.columns() {
                    let name = column.name();
                    let element = (
                        name,
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                        py.None(),
                    )
                        .into_pyobject(py)
                        .unwrap();
                    elements.push(element.into());
                }
                return Ok(Some(PyTuple::new(py, elements)?));
            }
        }
        Ok(None)
    }

    fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut rows_guard = rows_arc.lock().await;
            if let Some(rows) = rows_guard.as_mut() {
                let row = rows.next().await.map_err(to_py_err)?;
                Python::with_gil(|py| {
                    if let Some(row) = row {
                        let py_row = convert_row(py, row, rows.column_count())?;
                        Ok(py_row.into_any())
                    } else {
                        Ok(py.None().into_bound(py))
                    }
                })
            } else {
                Python::with_gil(|py| Ok(py.None().into_bound(py)))
            }
        })
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany<'py>(&self, py: Python<'py>, size: Option<i64>) -> PyResult<Bound<'py, PyAny>> {
        let size = size.unwrap_or(self.arraysize as i64);
        let rows_arc = self.rows.clone();
        let done_arc = self.done.clone();
        
        future_into_py(py, async move {
            let mut rows_guard = rows_arc.lock().await;
            let mut done_guard = done_arc.lock().await;
            if let Some(rows) = rows_guard.as_mut() {
                let mut all_rows = vec![];
                if !*done_guard {
                    for _ in 0..size {
                        let row = rows.next().await.map_err(to_py_err)?;
                        if let Some(r) = row {
                            all_rows.push(r);
                        } else {
                            *done_guard = true;
                            break;
                        }
                    }
                }
                Python::with_gil(|py| {
                    let mut elements: Vec<Py<PyAny>> = vec![];
                    for row in all_rows {
                        elements.push(convert_row(py, row, rows.column_count())?.into());
                    }
                    Ok(PyList::new(py, elements)?.into_any())
                })
            } else {
                Python::with_gil(|py| Ok(py.None().into_bound(py)))
            }
        })
    }

    fn fetchall<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rows_arc = self.rows.clone();
        future_into_py(py, async move {
            let mut rows_guard = rows_arc.lock().await;
            if let Some(rows) = rows_guard.as_mut() {
                let mut all_rows = vec![];
                while let Some(row) = rows.next().await.map_err(to_py_err)? {
                    all_rows.push(row);
                }
                Python::with_gil(|py| {
                    let mut elements: Vec<Py<PyAny>> = vec![];
                    for row in all_rows {
                        elements.push(convert_row(py, row, rows.column_count())?.into());
                    }
                    Ok(PyList::new(py, elements)?.into_any())
                })
            } else {
                Python::with_gil(|py| Ok(py.None().into_bound(py)))
            }
        })
    }

    #[getter]
    fn lastrowid(&self) -> PyResult<Option<i64>> {
        let stmt_guard = self.stmt.try_lock();
        if let Ok(stmt_ref) = stmt_guard {
            if stmt_ref.is_some() {
                let conn_guard = self.conn.try_lock();
                if let Ok(conn_ref) = conn_guard {
                    if let Some(conn) = conn_ref.as_ref() {
                        return Ok(Some(conn.last_insert_rowid()));
                    }
                }
            }
        }
        Ok(None)
    }

    #[getter]
    fn rowcount(&self) -> PyResult<i64> {
        let rowcount_guard = self.rowcount.try_lock();
        if let Ok(rc) = rowcount_guard {
            Ok(*rc)
        } else {
            Ok(0)
        }
    }
}

async fn begin_transaction(conn: &libsql_core::Connection) -> PyResult<()> {
    conn.execute("BEGIN", ()).await.map_err(to_py_err)?;
    Ok(())
}

fn extract_parameters(
    parameters: Option<ListOrTuple<'_>>
) -> PyResult<libsql_core::params::Params> {
    match parameters {
        Some(parameters) => {
            let mut params = vec![];
            for param in parameters.iter() {
                let param = if param.is_none() {
                    libsql_core::Value::Null
                } else if let Ok(value) = param.extract::<i32>() {
                    libsql_core::Value::Integer(value as i64)
                } else if let Ok(value) = param.extract::<f64>() {
                    libsql_core::Value::Real(value)
                } else if let Ok(value) = param.extract::<&str>() {
                    libsql_core::Value::Text(value.to_string())
                } else if let Ok(value) = param.extract::<&[u8]>() {
                    libsql_core::Value::Blob(value.to_vec())
                } else {
                    return Err(PyValueError::new_err(format!(
                        "Unsupported parameter type {}",
                        param.to_string()
                    )));
                };
                params.push(param);
            }
            Ok(libsql_core::params::Params::Positional(params))
        }
        None => Ok(libsql_core::params::Params::None),
    }
}

async fn execute_async(
    conn_arc: Arc<Mutex<Option<libsql_core::Connection>>>,
    stmt_arc: Arc<Mutex<Option<libsql_core::Statement>>>,
    rows_arc: Arc<Mutex<Option<libsql_core::Rows>>>,
    rowcount_arc: Arc<Mutex<i64>>,
    autocommit: i32,
    isolation_level: Option<String>,
    sql: String,
    params: libsql_core::params::Params,
) -> PyResult<()> {
    let mut conn_guard = conn_arc.lock().await;
    let conn = conn_guard.as_mut().ok_or_else(|| PyValueError::new_err("Connection already closed"))?;

    let stmt_is_dml = stmt_is_dml(&sql);
    let auto_c = determine_autocommit(autocommit, &isolation_level);
    if !auto_c && stmt_is_dml && conn.is_autocommit() {
        begin_transaction(conn).await?;
    }

    let mut stmt = conn.prepare(&sql).await.map_err(to_py_err)?;

    let mut rows_guard = rows_arc.lock().await;
    if stmt.columns().iter().len() > 0 {
        let rows = stmt.query(params).await.map_err(to_py_err)?;
        *rows_guard = Some(rows);
    } else {
        stmt.execute(params).await.map_err(to_py_err)?;
        *rows_guard = None;
    }

    let mut rowcount_guard = rowcount_arc.lock().await;
    *rowcount_guard += conn.changes() as i64;

    let mut stmt_guard = stmt_arc.lock().await;
    *stmt_guard = Some(stmt);

    Ok(())
}

fn determine_autocommit(autocommit: i32, isolation_level: &Option<String>) -> bool {
    if autocommit == LEGACY_TRANSACTION_CONTROL {
        isolation_level.is_none()
    } else {
        autocommit != 0
    }
}

fn stmt_is_dml(sql: &str) -> bool {
    let sql = sql.trim();
    let sql = sql.to_uppercase();
    sql.starts_with("INSERT") || sql.starts_with("UPDATE") || sql.starts_with("DELETE")
}

fn convert_row(
    py: Python,
    row: libsql_core::Row,
    column_count: i32,
) -> PyResult<Bound<'_, PyTuple>> {
    let mut elements: Vec<Py<PyAny>> = vec![];
    for col_idx in 0..column_count {
        let libsql_value = row.get_value(col_idx).map_err(to_py_err)?;
        let value = match libsql_value {
            libsql_core::Value::Integer(v) => {
                let value = v as i64;
                value.into_pyobject(py).unwrap().into()
            }
            libsql_core::Value::Real(v) => v.into_pyobject(py).unwrap().into(),
            libsql_core::Value::Text(v) => v.into_pyobject(py).unwrap().into(),
            libsql_core::Value::Blob(v) => {
                let value = v.as_slice();
                value.into_pyobject(py).unwrap().into()
            }
            libsql_core::Value::Null => py.None(),
        };
        elements.push(value);
    }
    Ok(PyTuple::new(py, elements)?)
}

create_exception!(libsql, Error, pyo3::exceptions::PyException);

impl<'py> FromPyObject<'py> for ListOrTuple<'py> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(list) = ob.downcast::<PyList>() {
            Ok(ListOrTuple::List(list.clone()))
        } else if let Ok(tuple) = ob.downcast::<PyTuple>() {
            Ok(ListOrTuple::Tuple(tuple.clone()))
        } else {
            Err(PyValueError::new_err(
                "Expected a list or tuple for parameters",
            ))
        }
    }
}

impl<'py> ListOrTuple<'py> {
    pub fn iter(&self) -> ListOrTupleIterator<'py> {
        ListOrTupleIterator {
            index: 0,
            inner: self.clone(),
        }
    }
}

impl<'py> Iterator for ListOrTupleIterator<'py> {
    type Item = Bound<'py, PyAny>;

    fn next(&mut self) -> Option<Self::Item> {
        let rv = match &self.inner {
            ListOrTuple::List(list) => list.get_item(self.index),
            ListOrTuple::Tuple(tuple) => tuple.get_item(self.index),
        };

        rv.ok().map(|item| {
            self.index += 1;
            item
        })
    }
}

#[pymodule]
fn aiolibsql(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = tracing_subscriber::fmt::try_init();
    m.add("LEGACY_TRANSACTION_CONTROL", LEGACY_TRANSACTION_CONTROL)?;
    m.add("paramstyle", "qmark")?;
    m.add("sqlite_version_info", (3, 42, 0))?;
    m.add("Error", py.get_type::<Error>())?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_class::<Connection>()?;
    m.add_class::<Cursor>()?;
    Ok(())
}
