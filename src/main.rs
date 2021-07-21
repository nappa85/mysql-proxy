use std::{convert::Infallible, env};

use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};

use serde_json::Value;

use warp::{Filter, Reply, hyper::StatusCode};

use mysql_async::{FromRowError, Params, Pool, Row, prelude::{FromRow, Queryable}};

use once_cell::sync::Lazy;

use log::error;

pub static POOL: Lazy<Pool> = Lazy::new(|| Pool::new(env::var("DATABASE_URL").expect("Missing env var DATABASE_URL").as_str()));

#[derive(Deserialize)]
#[serde(untagged)]
enum Query {
    Simple(String),
    Prepared((String, Vec<Value>)),
}

struct QueryRow(Row);

impl FromRow for QueryRow {
    fn from_row_opt(row: mysql_async::Row) -> Result<Self, FromRowError> {
        Ok(QueryRow(row))
    }
}

impl Serialize for QueryRow {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let columns = self.0.columns_ref();
        let mut map = serializer.serialize_map(Some(columns.len()))?;

        for (index, c) in columns.iter().enumerate() {
            match self.0.as_ref(index) {
                Some(mysql_async::Value::Bytes(a)) => map.serialize_entry(&c.name_str(), &String::from_utf8_lossy(a))?,
                Some(mysql_async::Value::Date(y, m, d, h, i, s, u)) => map.serialize_entry(&c.name_str(), &format!("{}-{}-{} {}:{}:{}.{}", y, m, d, h, i, s, u))?,
                Some(mysql_async::Value::Double(f)) => map.serialize_entry(&c.name_str(), f)?,
                Some(mysql_async::Value::Float(f)) => map.serialize_entry(&c.name_str(), f)?,
                Some(mysql_async::Value::Int(f)) => map.serialize_entry(&c.name_str(), f)?,
                Some(mysql_async::Value::UInt(f)) => map.serialize_entry(&c.name_str(), f)?,
                // Some(mysql_async::Value::Time(is_neg, d, h, m, s, u)) => Value::from(format!("{}-{}-{} {}:{}:{}.{}", y, m, d, h, i, s, u)),
                _ => map.serialize_entry(&c.name_str(), &Value::Null)?,
            }
        }

        map.end()
    }
}

fn convert_params(params: Vec<Value>) -> Params {
    Params::Positional(params.into_iter().map(|p| match p {
        Value::Bool(b) => mysql_async::Value::from(b),
        Value::String(s) => mysql_async::Value::from(s),
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                mysql_async::Value::from(f)
            }
            else if let Some(u) = n.as_u64() {
                mysql_async::Value::from(u)
            }
            else if let Some(i) = n.as_u64() {
                mysql_async::Value::from(i)
            }
            else {
                mysql_async::Value::NULL
            }
        },
        _ => mysql_async::Value::NULL,
    }).collect())
}

#[derive(Serialize)]
#[serde(untagged)]
enum QueryResult {
    Id(Option<u64>),
    Rows(Vec<QueryRow>),
}

fn error<E: std::error::Error>(prefix: &str, e: E) -> String {
    error!("{}: {}", prefix, e);
    e.to_string()
}

async fn _query(query: Query, get_id: bool) -> Result<QueryResult, String> {
    let mut conn = POOL.get_conn().await.map_err(|e| error("MySQL connection error", e))?;
    if get_id {
        match query {
            Query::Simple(q) => Ok(QueryResult::Id(conn.query_iter(q).await.map_err(|e| error("MySQL query error", e))?.last_insert_id())),
            Query::Prepared((q, p)) => Ok(QueryResult::Id(conn.exec_iter(q, convert_params(p)).await.map_err(|e| error("MySQL query error", e))?.last_insert_id())),
        }
    }
    else {
        match query {
            Query::Simple(q) => Ok(QueryResult::Rows(conn.query(q).await.map_err(|e| error("MySQL query error", e))?)),
            Query::Prepared((q, p)) => Ok(QueryResult::Rows(conn.exec(q, convert_params(p)).await.map_err(|e| error("MySQL query error", e))?)),
        }
    }
}

#[derive(Deserialize)]
struct CallParams {
    #[serde(rename = "return")]
    _return: Option<bool>,
}

async fn query(q: Query, params: CallParams) -> Result<impl warp::Reply, Infallible> {
    Ok(match _query(q, params._return == Some(true)).await {
        Ok(v) => warp::reply::json(&v).into_response(),
        Err(e) => warp::reply::with_status(e, StatusCode::INTERNAL_SERVER_ERROR).into_response(),
    })
}

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let promote = warp::post()
        .and(warp::body::json())
        .and(warp::filters::query::query())
        .and_then(query);

    warp::serve(promote).run(([0, 0, 0, 0], 3030)).await;
}
