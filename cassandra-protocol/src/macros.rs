#[macro_export]

/// Transforms arguments to values consumed by queries.
macro_rules! query_values {
    ($($value:expr),*) => {
        {
            use cassandra_protocol::types::value::Value;
            use cassandra_protocol::query::QueryValues;
            let mut values: Vec<Value> = Vec::new();
            $(
                values.push($value.into());
            )*
            QueryValues::SimpleValues(values)
        }
    };
    ($($name:expr => $value:expr),*) => {
        {
            use cassandra_protocol::types::value::Value;
            use cassandra_protocol::query::QueryValues;
            use std::collections::HashMap;
            let mut values: HashMap<String, Value> = HashMap::new();
            $(
                values.insert($name.to_string(), $value.into());
            )*
            QueryValues::NamedValues(values)
        }
    };
}

macro_rules! list_as_rust {
    (List) => (
        impl AsRustType<Vec<List>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<List>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let protocol_version = self.protocol_version;
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, protocol_version, List)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
    (Map) => (
        impl AsRustType<Vec<Map>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<Map>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let protocol_version = self.protocol_version;
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, protocol_version, Map)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
    (Udt) => (
        impl AsRustType<Vec<Udt>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<Udt>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let protocol_version = self.protocol_version;
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, protocol_version, Udt)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
    (Tuple) => (
        impl AsRustType<Vec<Tuple>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<Tuple>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let protocol_version = self.protocol_version;
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, protocol_version, Tuple)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
    ($($into_type:tt)+) => (
        impl AsRustType<Vec<$($into_type)+>> for List {
            fn as_rust_type(&self) -> Result<Option<Vec<$($into_type)+>>> {
                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option)) |
                    Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.as_ref();
                        let convert = self
                            .map(|bytes| {
                                as_rust_type!(type_option_ref, bytes, $($into_type)+)
                                    .unwrap()
                                    // item in a list supposed to be a non-null value.
                                    // TODO: check if it's true
                                    .unwrap()
                            });

                        Ok(Some(convert))
                    },
                    _ => Err(Error::General(format!("Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                            self.metadata.value)))
                }
            }
        }
    );
}

macro_rules! list_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for List {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::error::Error;
                use crate::types::cassandra_type::wrapper_fn;
                use crate::types::cassandra_type::CassandraType;
                use std::ops::Deref;

                let protocol_version = self.protocol_version;

                match self.metadata.value {
                    Some(ColTypeOptionValue::CList(ref type_option))
                    | Some(ColTypeOptionValue::CSet(ref type_option)) => {
                        let type_option_ref = type_option.deref().clone();
                        let wrapper = wrapper_fn(&type_option_ref.id);
                        let convert = self.map(|bytes| {
                            wrapper(bytes, &type_option_ref, protocol_version).unwrap()
                        });
                        Ok(Some(CassandraType::List(convert)))
                    }
                    _ => Err(Error::General(format!(
                        "Invalid conversion. \
                            Cannot convert {:?} into List (valid types: List, Set).",
                        self.metadata.value
                    ))),
                }
            }
        }
    };
}

macro_rules! map_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for Map {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::types::cassandra_type::wrapper_fn;
                use crate::types::cassandra_type::CassandraType;
                use std::ops::Deref;

                if let Some(ColTypeOptionValue::CMap(
                    ref key_col_type_option,
                    ref value_col_type_option,
                )) = self.metadata.value
                {
                    let key_col_type_option = key_col_type_option.deref().clone();
                    let value_col_type_option = value_col_type_option.deref().clone();

                    let key_wrapper = wrapper_fn(&key_col_type_option.id);

                    let value_wrapper = wrapper_fn(&value_col_type_option.id);

                    let protocol_version = self.protocol_version;

                    let map = self
                        .data
                        .iter()
                        .map(|(key, value)| {
                            (
                                key_wrapper(key, &key_col_type_option, protocol_version).unwrap(),
                                value_wrapper(value, &value_col_type_option, protocol_version)
                                    .unwrap(),
                            )
                        })
                        .collect::<Vec<(CassandraType, CassandraType)>>();

                    return Ok(Some(CassandraType::Map(map)));
                } else {
                    panic!("not  amap")
                }
            }
        }
    };
}

macro_rules! tuple_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for Tuple {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::types::cassandra_type::wrapper_fn;
                use crate::types::cassandra_type::CassandraType;

                let protocol_version = self.protocol_version;
                let values = self
                    .data
                    .iter()
                    .map(|(col_type, bytes)| {
                        let wrapper = wrapper_fn(&col_type.id);
                        wrapper(&bytes, col_type, protocol_version).unwrap()
                    })
                    .collect();

                Ok(Some(CassandraType::Tuple(values)))
            }
        }
    };
}

macro_rules! udt_as_cassandra_type {
    () => {
        impl crate::types::AsCassandraType for Udt {
            fn as_cassandra_type(
                &self,
            ) -> Result<Option<crate::types::cassandra_type::CassandraType>> {
                use crate::types::cassandra_type::wrapper_fn;
                use crate::types::cassandra_type::CassandraType;
                use std::collections::HashMap;

                let mut map = HashMap::with_capacity(self.data.len());
                let protocol_version = self.protocol_version;

                self.data.iter().for_each(|(key, (col_type, bytes))| {
                    let wrapper = wrapper_fn(&col_type.id);
                    let value = wrapper(&bytes, col_type, protocol_version).unwrap();
                    map.insert(key.clone(), value);
                });

                Ok(Some(CassandraType::Udt(map)))
            }
        }
    };
}

macro_rules! map_as_rust {
    ({ Tuple }, { List }) => (
        impl AsRustType<HashMap<Tuple, List>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<Tuple, List>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, protocol_version, Tuple)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, List)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ Tuple }, { Map }) => (
        impl AsRustType<HashMap<Tuple, Map>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<Tuple, Map>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, protocol_version, Tuple)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Map)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ Tuple }, { Udt }) => (
        impl AsRustType<HashMap<Tuple, Udt>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<Tuple, Udt>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, protocol_version, Tuple)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Udt)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ Tuple }, { Tuple }) => (
        impl AsRustType<HashMap<Tuple, Tuple>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<Tuple, Tuple>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, protocol_version, Tuple)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Tuple)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ Tuple }, { $($val_type:tt)+ }) => (
        impl AsRustType<HashMap<Tuple, $($val_type)+>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<Tuple, $($val_type)+>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, protocol_version, Tuple)?;
                            let val = as_rust_type!(val_type_option, val, $($val_type)+)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ $($key_type:tt)+ }, { List }) => (
        impl AsRustType<HashMap<$($key_type)+, List>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, List>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, List)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ $($key_type:tt)+ }, { Map }) => (
        impl AsRustType<HashMap<$($key_type)+, Map>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, Map>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Map)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ $($key_type:tt)+ }, { Udt }) => (
        impl AsRustType<HashMap<$($key_type)+, Udt>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, Udt>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Udt)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ $($key_type:tt)+ }, { Tuple }) => (
        impl AsRustType<HashMap<$($key_type)+, Tuple>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, Tuple>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();
                        let protocol_version = self.protocol_version;

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, protocol_version, Tuple)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
    ({ $($key_type:tt)+ }, { $($val_type:tt)+ }) => (
        impl AsRustType<HashMap<$($key_type)+, $($val_type)+>> for Map {
            /// Converts `Map` into `HashMap` for blob values.
            fn as_rust_type(&self) -> Result<Option<HashMap<$($key_type)+, $($val_type)+>>> {
                if let Some(ColTypeOptionValue::CMap(key_type_option, val_type_option)) = &self.metadata.value {
                    let mut map = HashMap::with_capacity(self.data.len());
                        let key_type_option = key_type_option.as_ref();
                        let val_type_option = val_type_option.as_ref();

                        for (key, val) in self.data.iter() {
                            let key = as_rust_type!(key_type_option, key, $($key_type)+)?;
                            let val = as_rust_type!(val_type_option, val, $($val_type)+)?;
                            if let (Some(key), Some(val)) = (key, val) {
                                map.insert(key, val);
                            }
                        }

                        Ok(Some(map))
                } else {
                    Err(format!("Invalid column type for map: {:?}", self.metadata.value).into())
                }
            }
        }
    );
}

macro_rules! into_rust_by_name {
    (Row, List) => (
        impl IntoRustByName<List> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<List>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, List)
                    })
            }
        }
    );
    (Row, Map) => (
        impl IntoRustByName<Map> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<Map>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Map)
                    })
            }
        }
    );
    (Row, Udt) => (
        impl IntoRustByName<Udt> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<Udt>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Udt)
                    })
            }
        }
    );
    (Row, Tuple) => (
        impl IntoRustByName<Tuple> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<Tuple>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Tuple)
                    })
            }
        }
    );
    (Row, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for Row {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.col_spec_by_name(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
    (Udt, List) => (
        impl IntoRustByName<List> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<List>> {
                let protocol_version = self.protocol_version;
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, List);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Udt, Map) => (
        impl IntoRustByName<Map> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<Map>> {
                let protocol_version = self.protocol_version;
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Map);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Udt, Udt) => (
        impl IntoRustByName<Udt> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<Udt>> {
                let protocol_version = self.protocol_version;
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Udt);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Udt, Tuple) => (
        impl IntoRustByName<Tuple> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<Tuple>> {
                let protocol_version = self.protocol_version;
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Tuple);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Udt, $($into_type:tt)+) => (
        impl IntoRustByName<$($into_type)+> for Udt {
            fn get_by_name(&self, name: &str) -> Result<Option<$($into_type)+>> {
                self.data.get(name)
                    .ok_or(column_is_empty_err(name))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
}

macro_rules! into_rust_by_index {
    (Tuple, List) => (
        impl IntoRustByIndex<List> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<List>> {
                let protocol_version = self.protocol_version;
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, List);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Tuple, Map) => (
        impl IntoRustByIndex<Map> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<Map>> {
                let protocol_version = self.protocol_version;
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Map);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Tuple, Udt) => (
        impl IntoRustByIndex<Udt> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<Udt>> {
                let protocol_version = self.protocol_version;
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Udt);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Tuple, Tuple) => (
        impl IntoRustByIndex<Tuple> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<Tuple>> {
                let protocol_version = self.protocol_version;
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, protocol_version, Tuple);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Tuple, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Tuple {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.data
                    .get(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|v| {
                        let &(ref col_type, ref bytes) = v;
                        let converted = as_rust_type!(col_type, bytes, $($into_type)+);
                        converted.map_err(|err| err.into())
                    })
            }
        }
    );
    (Row, List) => (
        impl IntoRustByIndex<List> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<List>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, List)
                    })
            }
        }
    );
    (Row, Map) => (
        impl IntoRustByIndex<Map> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<Map>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Map)
                    })
            }
        }
    );
    (Row, Udt) => (
        impl IntoRustByIndex<Udt> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<Udt>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Udt)
                    })
            }
        }
    );
    (Row, Tuple) => (
        impl IntoRustByIndex<Tuple> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<Tuple>> {
                let protocol_version = self.protocol_version;
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, protocol_version, Tuple)
                    })
            }
        }
    );
    (Row, $($into_type:tt)+) => (
        impl IntoRustByIndex<$($into_type)+> for Row {
            fn get_by_index(&self, index: usize) -> Result<Option<$($into_type)+>> {
                self.col_spec_by_index(index)
                    .ok_or(column_is_empty_err(index))
                    .and_then(|(col_spec, cbytes)| {
                        let col_type = &col_spec.col_type;
                        as_rust_type!(col_type, cbytes, $($into_type)+)
                    })
            }
        }
    );
}

macro_rules! as_res_opt {
    ($data_value:ident, $deserialize:expr) => {
        match $data_value.as_slice() {
            Some(ref bytes) => ($deserialize)(bytes).map(Some).map_err(Into::into),
            None => Ok(None),
        }
    };
}

/// Decodes any Cassandra data type into the corresponding Rust type,
/// given the column type as `ColTypeOption` and the value as `CBytes`
/// plus the matching Rust type.
macro_rules! as_rust_type {
    ($data_type_option:ident, $data_value:ident, Blob) => {

        match $data_type_option.id {
            ColType::Blob => as_res_opt!($data_value, decode_blob),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(crate::envelope::message_result::ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.BytesType" {
                            return as_res_opt!($data_value, decode_blob);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into Vec<u8> (valid types: org.apache.cassandra.db.marshal.BytesType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Vec<u8> (valid types: Blob).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, String) => {
        match $data_type_option.id {
            ColType::Custom => as_res_opt!($data_value, decode_custom),
            ColType::Ascii => as_res_opt!($data_value, decode_ascii),
            ColType::Varchar => as_res_opt!($data_value, decode_varchar),
            // TODO: clarify when to use decode_text.
            // it's not mentioned in
            // https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L582
            // ColType::XXX => decode_text($data_value)?
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into String (valid types: Custom, Ascii, Varchar).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, bool) => {
        match $data_type_option.id {
            ColType::Boolean => as_res_opt!($data_value, decode_boolean),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.BooleanType" {
                            return as_res_opt!($data_value, decode_boolean);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into bool (valid types: org.apache.cassandra.db.marshal.BooleanType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into bool (valid types: Boolean, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i64) => {
        match $data_type_option.id {
            ColType::Bigint => as_res_opt!($data_value, decode_bigint),
            ColType::Timestamp => as_res_opt!($data_value, decode_timestamp),
            ColType::Time => as_res_opt!($data_value, decode_time),
            ColType::Counter => as_res_opt!($data_value, decode_bigint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.LongType" | "org.apache.cassandra.db.marshal.CounterColumnType" => return as_res_opt!($data_value, decode_bigint),
                            "org.apache.cassandra.db.marshal.TimestampType" => return as_res_opt!($data_value, decode_timestamp),
                            "org.apache.cassandra.db.marshal.TimeType" => return as_res_opt!($data_value, decode_time),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i64 (valid types: org.apache.cassandra.db.marshal.{{LongType|IntegerType|CounterColumnType|TimestampType|TimeType}}).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i64 (valid types: Bigint, Timestamp, Time,\
                 Counter, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i32) => {
        match $data_type_option.id {
            ColType::Int => as_res_opt!($data_value, decode_int),
            ColType::Date => as_res_opt!($data_value, decode_date),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(crate::envelope::message_result::ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.Int32Type" => return as_res_opt!($data_value, decode_int),
                            "org.apache.cassandra.db.marshal.SimpleDateType" => return as_res_opt!($data_value, decode_date),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i32 (valid types: org.apache.cassandra.db.marshal.Int32Type).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i32 (valid types: Int, Date, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i16) => {
        match $data_type_option.id {
            ColType::Smallint => as_res_opt!($data_value, decode_smallint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ShortType" {
                            return as_res_opt!($data_value, decode_smallint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i16 (valid types: org.apache.cassandra.db.marshal.ShortType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i16 (valid types: Smallint, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, i8) => {
        match $data_type_option.id {
            ColType::Tinyint => as_res_opt!($data_value, decode_tinyint),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ByteType" {
                            return as_res_opt!($data_value, decode_tinyint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i8 (valid types: org.apache.cassandra.db.marshal.ByteType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into i8 (valid types: Tinyint, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI64) => {
        match $data_type_option.id {
            ColType::Bigint => {
                as_res_opt!($data_value, decode_bigint).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Timestamp => as_res_opt!($data_value, decode_timestamp)
                .map(|value| value.and_then(NonZeroI64::new)),
            ColType::Time => {
                as_res_opt!($data_value, decode_time).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Counter => {
                as_res_opt!($data_value, decode_bigint).map(|value| value.and_then(NonZeroI64::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.LongType" | "org.apache.cassandra.db.marshal.CounterColumnType" => return as_res_opt!($data_value, decode_bigint),
                            "org.apache.cassandra.db.marshal.TimestampType" => return as_res_opt!($data_value, decode_timestamp),
                            "org.apache.cassandra.db.marshal.TimeType" => return as_res_opt!($data_value, decode_time),
                            _ => {}
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i64 (valid types: org.apache.cassandra.db.marshal.{{LongType|IntegerType|CounterColumnType|TimestampType|TimeType}}).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI64::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NonZeroI64 (valid types: Bigint, Timestamp, Time,\
                 Counter, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI32) => {
        match $data_type_option.id {
            ColType::Int => {
                as_res_opt!($data_value, decode_int).map(|value| value.and_then(NonZeroI32::new))
            }
            ColType::Date => {
                as_res_opt!($data_value, decode_date).map(|value| value.and_then(NonZeroI32::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.Int32Type" => return as_res_opt!($data_value, decode_int),
                            "org.apache.cassandra.db.marshal.SimpleDateType" => return as_res_opt!($data_value, decode_date),
                            _ => {}
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into i32 (valid types: org.apache.cassandra.db.marshal.Int32Type).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI32::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NonZeroI32 (valid types: Int, Date, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI16) => {
        match $data_type_option.id {
            ColType::Smallint => as_res_opt!($data_value, decode_smallint)
                .map(|value| value.and_then(NonZeroI16::new)),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ShortType" {
                            return as_res_opt!($data_value, decode_smallint);
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into NonZeroI16 (valid types: org.apache.cassandra.db.marshal.ShortType).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI16::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NonZeroI16 (valid types: Smallint, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NonZeroI8) => {
        match $data_type_option.id {
            ColType::Tinyint => {
                as_res_opt!($data_value, decode_tinyint).map(|value| value.and_then(NonZeroI8::new))
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.ByteType" {
                            return as_res_opt!($data_value, decode_tinyint);
                        }
                    }

                    Err(Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into NonZeroI8 (valid types: org.apache.cassandra.db.marshal.ByteType).",
                        $data_type_option
                    )))
                };

                unmarshal().map(|value| value.and_then(NonZeroI8::new))
            }
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NonZeroI8 (valid types: Tinyint, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f64) => {
        match $data_type_option.id {
            ColType::Double => as_res_opt!($data_value, decode_double),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.DoubleType" {
                            return as_res_opt!($data_value, decode_double);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into f64 (valid types: org.apache.cassandra.db.marshal.DoubleType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f64 (valid types: Double, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, f32) => {
        match $data_type_option.id {
            ColType::Float => as_res_opt!($data_value, decode_float),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.FloatType" {
                            return as_res_opt!($data_value, decode_float);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into f32 (valid types: org.apache.cassandra.db.marshal.FloatType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into f32 (valid types: Float, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, IpAddr) => {
        match $data_type_option.id {
            ColType::Inet => as_res_opt!($data_value, decode_inet),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.InetAddressType" {
                            return as_res_opt!($data_value, decode_inet);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into IpAddr (valid types: org.apache.cassandra.db.marshal.InetAddressType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into IpAddr (valid types: Inet, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Uuid) => {
        match $data_type_option.id {
            ColType::Uuid | ColType::Timeuuid => as_res_opt!($data_value, decode_timeuuid),
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        match value.as_str() {
                            "org.apache.cassandra.db.marshal.UUIDType" | "org.apache.cassandra.db.marshal.TimeUUIDType" => return as_res_opt!($data_value, decode_timeuuid),
                            _ => {}
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshaled type {:?} into Uuid (valid types: org.apache.cassandra.db.marshal.{{UUIDType|TimeUUIDType}}).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Uuid (valid types: Uuid, Timeuuid, Custom).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, $version:ident, List) => {
        match $data_type_option.id {
            ColType::List | ColType::Set => match $data_value.as_slice() {
                Some(ref bytes) => decode_list(bytes, $version)
                    .map(|data| Some(List::new($data_type_option.clone(), data, $version)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into List (valid types: List, Set).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, $version:ident, Map) => {
        match $data_type_option.id {
            ColType::Map => match $data_value.as_slice() {
                Some(ref bytes) => decode_map(bytes, $version)
                    .map(|data| Some(Map::new(data, $data_type_option.clone(), $version)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Map (valid types: Map).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, $version:ident, Udt) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Udt,
                value: Some(ColTypeOptionValue::UdtType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_udt(bytes, list_type_option.descriptions.len(), $version)
                    .map(|data| Some(Udt::new(data, list_type_option, $version)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Udt (valid types: Udt).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, $version:ident, Tuple) => {
        match *$data_type_option {
            ColTypeOption {
                id: ColType::Tuple,
                value: Some(ColTypeOptionValue::TupleType(ref list_type_option)),
            } => match $data_value.as_slice() {
                Some(ref bytes) => decode_tuple(bytes, list_type_option.types.len(), $version)
                    .map(|data| Some(Tuple::new(data, list_type_option, $version)))
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Tuple (valid types: Tuple).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, PrimitiveDateTime) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        let unix_epoch = time::macros::date!(1970 - 01 - 01).midnight();
                        let tm = unix_epoch
                            + time::Duration::new(ts / 1_000, (ts % 1_000 * 1_000_000) as i32);
                        Some(tm)
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into PrimitiveDateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Decimal) => {
        match $data_type_option.id {
            ColType::Decimal => match $data_value.as_slice() {
                Some(ref bytes) => decode_decimal(bytes).map(Some).map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Decimal (valid types: Decimal).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, NaiveDateTime) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        NaiveDateTime::from_timestamp_opt(ts / 1000, (ts % 1000 * 1_000_000) as u32)
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into NaiveDateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, DateTime<Utc>) => {
        match $data_type_option.id {
            ColType::Timestamp => match $data_value.as_slice() {
                Some(ref bytes) => decode_timestamp(bytes)
                    .map(|ts| {
                        Some(DateTime::from_utc(
                            NaiveDateTime::from_timestamp_opt(
                                ts / 1000,
                                (ts % 1000 * 1_000_000) as u32,
                            )?,
                            Utc,
                        ))
                    })
                    .map_err(Into::into),
                None => Ok(None),
            },
            _ => Err(Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into DateTime (valid types: Timestamp).",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, BigInt) => {
        match $data_type_option.id {
            ColType::Varint => {
                as_res_opt!($data_value, decode_varint)
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.IntegerType" {
                            return as_res_opt!($data_value, decode_varint);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshalled type {:?} into BigInt (valid types: org.apache.cassandra.db.marshal.IntegerType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into BigInt (valid types: Varint, Custom)",
                $data_type_option.id
            ))),
        }
    };
    ($data_type_option:ident, $data_value:ident, Duration) => {
        match $data_type_option.id {
            ColType::Duration => {
                as_res_opt!($data_value, decode_duration)
            }
            ColType::Custom => {
                let unmarshal = || {
                    if let Some(ColTypeOptionValue::CString(value)) = &$data_type_option.value {
                        if value.as_str() == "org.apache.cassandra.db.marshal.DurationType" {
                            return as_res_opt!($data_value, decode_duration);
                        }
                    }

                    Err(crate::error::Error::General(format!(
                        "Invalid conversion. \
                         Cannot convert marshalled type {:?} into Duration (valid types: org.apache.cassandra.db.marshal.DurationType).",
                        $data_type_option
                    )))
                };

                unmarshal()
            }
            _ => Err(crate::error::Error::General(format!(
                "Invalid conversion. \
                 Cannot convert {:?} into Duration (valid types: Duration, Custom)",
                $data_type_option.id
            ))),
        }
    };
}
