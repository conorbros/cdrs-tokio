use crate::envelope::message_result::{CTuple, ColType, ColTypeOption, ColTypeOptionValue};
use crate::envelope::Version;
use crate::error::{column_is_empty_err, Error, Result};
use crate::types::blob::Blob;
use crate::types::data_serialization_types::*;
use crate::types::decimal::Decimal;
use crate::types::list::List;
use crate::types::map::Map;
use crate::types::udt::Udt;
use crate::types::{ByIndex, CBytes, IntoRustByIndex};
use chrono::prelude::*;
use num::BigInt;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use time::PrimitiveDateTime;
use uuid::Uuid;

#[derive(Debug)]
pub struct Tuple {
    data: Vec<(ColTypeOption, CBytes)>,
    protocol_version: Version,
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Tuple) -> bool {
        if self.data.len() != other.data.len() {
            return false;
        }
        for (s, o) in self.data.iter().zip(other.data.iter()) {
            if s.1 != o.1 {
                return false;
            }
        }
        true
    }
}

impl Eq for Tuple {}

impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for data in &self.data {
            data.1.hash(state);
        }
    }
}

impl Tuple {
    pub fn new(elements: Vec<CBytes>, metadata: &CTuple, protocol_version: Version) -> Tuple {
        Tuple {
            data: metadata
                .types
                .iter()
                .zip(elements.into_iter())
                .map(|(val_type, val_b)| (val_type.clone(), val_b))
                .collect(),
            protocol_version,
        }
    }
}

impl ByIndex for Tuple {}

into_rust_by_index!(Tuple, Blob);
into_rust_by_index!(Tuple, String);
into_rust_by_index!(Tuple, bool);
into_rust_by_index!(Tuple, i64);
into_rust_by_index!(Tuple, i32);
into_rust_by_index!(Tuple, i16);
into_rust_by_index!(Tuple, i8);
into_rust_by_index!(Tuple, f64);
into_rust_by_index!(Tuple, f32);
into_rust_by_index!(Tuple, IpAddr);
into_rust_by_index!(Tuple, Uuid);
into_rust_by_index!(Tuple, List);
into_rust_by_index!(Tuple, Map);
into_rust_by_index!(Tuple, Udt);
into_rust_by_index!(Tuple, Tuple);
into_rust_by_index!(Tuple, PrimitiveDateTime);
into_rust_by_index!(Tuple, Decimal);
into_rust_by_index!(Tuple, NaiveDateTime);
into_rust_by_index!(Tuple, DateTime<Utc>);
into_rust_by_index!(Tuple, BigInt);

tuple_as_cassandra_type!();
