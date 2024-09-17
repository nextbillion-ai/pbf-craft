use crate::pbf::proto::osmformat::PrimitiveBlock;
use chrono::{DateTime, Utc};

pub struct FieldCodec {
    date_granularity: i32,
    granularity: i32,
    lat_offset: i64,
    lon_offset: i64,
    string_table: Vec<String>,
}

impl FieldCodec {
    pub fn new(granularity: i32, date_granularity: i32) -> Self {
        Self {
            date_granularity,
            granularity,
            lat_offset: 0,
            lon_offset: 0,
            string_table: Vec::new(),
        }
    }

    pub fn new_with_block(block: &PrimitiveBlock) -> Self {
        let bytes_array = block.get_stringtable().get_s();
        let string_table = if bytes_array.is_empty() {
            Vec::with_capacity(0)
        } else {
            bytes_array
                .into_iter()
                .map(|bytes| match String::from_utf8(bytes.clone()) {
                    Ok(str) => str,
                    Err(err) => {
                        eprintln!("{}", err);
                        String::new()
                    }
                })
                .collect::<Vec<String>>()
        };
        Self {
            date_granularity: block.get_date_granularity(),
            granularity: block.get_granularity(),
            lat_offset: block.get_lat_offset(),
            lon_offset: block.get_lon_offset(),
            string_table,
        }
    }

    pub fn encode_latitude(&self, latitude: i64) -> i64 {
        (latitude - self.lat_offset) / self.granularity as i64
    }

    pub fn decode_latitude(&self, raw_latitude: i64) -> i64 {
        self.lat_offset + (self.granularity as i64 * raw_latitude)
    }

    pub fn encode_longitude(&self, longitude: i64) -> i64 {
        (longitude - self.lon_offset) / self.granularity as i64
    }

    pub fn decode_longitude(&self, raw_longitude: i64) -> i64 {
        self.lon_offset + (self.granularity as i64 * raw_longitude)
    }

    pub fn encode_timestamp(&self, time: DateTime<Utc>) -> i64 {
        time.timestamp_millis() / self.date_granularity as i64
    }

    pub fn decode_timestamp(&self, raw_timestamp: i64) -> DateTime<Utc> {
        let timestamp = self.date_granularity as i64 * raw_timestamp;
        return DateTime::from_timestamp_millis(timestamp).expect("invalid timestamp");
    }

    pub fn decode_string(&self, string_id: usize) -> String {
        match self.string_table.get(string_id) {
            None => {
                eprintln!("no matched string table id: {}", string_id);
                String::new()
            }
            Some(s) => s.to_owned(),
        }
    }
}
