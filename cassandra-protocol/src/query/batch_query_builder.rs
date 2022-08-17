use crate::consistency::Consistency;
use crate::envelope::message_batch::{BatchQuery, BatchQuerySubj, BatchType, BodyReqBatch};
use crate::error::{Error as CError, Result as CResult};
use crate::query::{PreparedQuery, QueryValues};
use crate::types::{CInt, CLong};

pub type QueryBatch = BodyReqBatch;

#[derive(Debug)]
pub struct BatchQueryBuilder {
    batch_type: BatchType,
    queries: Vec<BatchQuery>,
    consistency: Consistency,
    serial_consistency: Option<Consistency>,
    timestamp: Option<CLong>,
    keyspace: Option<String>,
    now_in_seconds: Option<CInt>,
}

impl Default for BatchQueryBuilder {
    fn default() -> Self {
        BatchQueryBuilder {
            batch_type: BatchType::Logged,
            queries: vec![],
            consistency: Consistency::One,
            serial_consistency: None,
            timestamp: None,
            keyspace: None,
            now_in_seconds: None,
        }
    }
}

impl BatchQueryBuilder {
    pub fn new() -> BatchQueryBuilder {
        Default::default()
    }

    #[must_use]
    pub fn with_batch_type(mut self, batch_type: BatchType) -> Self {
        self.batch_type = batch_type;
        self
    }

    /// Add a query (non-prepared one)
    #[must_use]
    pub fn add_query<T: Into<String>>(mut self, query: T, values: QueryValues) -> Self {
        self.queries.push(BatchQuery {
            subject: BatchQuerySubj::QueryString(query.into()),
            values,
        });
        self
    }

    /// Add a query (prepared one)
    #[must_use]
    pub fn add_query_prepared(mut self, query: &PreparedQuery, values: QueryValues) -> Self {
        self.queries.push(BatchQuery {
            subject: BatchQuerySubj::PreparedId(query.id.clone()),
            values,
        });
        self
    }

    #[must_use]
    pub fn clear_queries(mut self) -> Self {
        self.queries = vec![];
        self
    }

    #[must_use]
    pub fn with_consistency(mut self, consistency: Consistency) -> Self {
        self.consistency = consistency;
        self
    }

    #[must_use]
    pub fn with_serial_consistency(mut self, serial_consistency: Consistency) -> Self {
        self.serial_consistency = Some(serial_consistency);
        self
    }

    #[must_use]
    pub fn with_timestamp(mut self, timestamp: CLong) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    #[must_use]
    pub fn with_keyspace(mut self, keyspace: String) -> Self {
        self.keyspace = Some(keyspace);
        self
    }

    #[must_use]
    pub fn with_now_in_seconds(mut self, now_in_seconds: CInt) -> Self {
        self.now_in_seconds = Some(now_in_seconds);
        self
    }

    pub fn build(self) -> CResult<BodyReqBatch> {
        let with_names_for_values = self.queries.iter().all(|q| q.values.has_names());

        if !with_names_for_values {
            let some_names_for_values = self.queries.iter().any(|q| q.values.has_names());

            if some_names_for_values {
                return Err(CError::General(String::from(
                    "Inconsistent query values - mixed \
                     with and without names values",
                )));
            }
        }

        Ok(BodyReqBatch {
            batch_type: self.batch_type,
            queries: self.queries,
            consistency: self.consistency,
            serial_consistency: self.serial_consistency,
            timestamp: self.timestamp,
            keyspace: self.keyspace,
            now_in_seconds: self.now_in_seconds,
        })
    }
}
