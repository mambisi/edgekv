use anyhow::Result;
use std::io::{Read};
use crc32fast::Hasher;

pub(crate) fn crc_checksum<P : AsRef<[u8]>>(payload : P) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(payload.as_ref());
    hasher.finalize()
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub(crate)  struct DataEntry {
    crc: u32,
    level: i64,
    key_size: u64,
    value_size: u64,
    key: Vec<u8>,
    value: Vec<u8>,
}

pub(crate)  trait Encoder {
    fn encode(&self) -> Vec<u8>;
}

pub(crate)  trait Decoder {
    fn decode<R: Read>(rdr: &mut R) -> Result<Self> where Self: Sized;
}

impl Encoder for DataEntry {
    fn encode(&self) -> Vec<u8> {
        let content = self.encode_content();
        let crc = crc_checksum(&content);
        let mut buf = vec![];
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&content);
        return buf;
    }
}

impl Decoder for DataEntry {
    fn decode<R: Read>(rdr: &mut R) -> Result<Self> where Self: Sized {
        let mut out = Self {
            crc: 0,
            level: 0,
            key_size: 0,
            value_size: 0,
            key: vec![],
            value: vec![],
        };
        let mut raw_crc_bytes = [0_u8; 4];
        let mut raw_level_bytes = [0_u8; 8];
        let mut raw_key_size_bytes = [0_u8; 8];
        let mut raw_value_size_bytes = [0_u8; 8];

        rdr.read_exact(&mut raw_crc_bytes)?;
        rdr.read_exact(&mut raw_level_bytes)?;
        rdr.read_exact(&mut raw_key_size_bytes)?;
        rdr.read_exact(&mut raw_value_size_bytes)?;

        out.crc = u32::from_be_bytes(raw_crc_bytes);
        out.level = i64::from_be_bytes(raw_level_bytes);
        out.key_size = u64::from_be_bytes(raw_key_size_bytes);
        out.value_size = u64::from_be_bytes(raw_value_size_bytes);

        let mut raw_key_bytes = vec![0_u8; out.key_size as usize];
        let mut raw_value_bytes = vec![0_u8; out.value_size as usize];

        rdr.read_exact(&mut raw_key_bytes);
        rdr.read_exact(&mut raw_value_bytes);

        out.key = raw_key_bytes;
        out.value = raw_value_bytes;

        Ok(out)
    }
}

impl DataEntry {
    pub(crate)  fn new(level: i64, key: Vec<u8>, value: Vec<u8>) -> Self {
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;

        Self {
            crc: 0,
            level,
            key_size,
            value_size,
            key,
            value,
        }
    }

    pub fn check_crc(&self) -> bool {
        self.crc == crc_checksum(&self.encode_content())
    }

    fn encode_content(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(&self.level.to_be_bytes());
        buf.extend_from_slice(&self.key_size.to_be_bytes());
        buf.extend_from_slice(&self.value_size.to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }

    pub(crate)  fn key(&self) -> Vec<u8> {
        self.key.to_owned()
    }
    pub(crate)  fn value(&self) -> Vec<u8> {
        self.value.to_owned()
    }

}

pub(crate) struct HintEntry {
    level: i64,
    key_size: u64,
    value_size: u64,
    data_entry_position: u64,
    key: Vec<u8>,
}

impl HintEntry {
    pub(crate)  fn from(entry: &DataEntry, position: u64) -> Self {
        Self {
            level: entry.level,
            key_size: entry.key_size,
            value_size: entry.value_size,
            data_entry_position: position,
            key: entry.key.clone(),
        }
    }
    pub(crate)  fn tombstone(key : Vec<u8>) -> Self {
        Self {
            level: -1,
            key_size: key.len() as u64,
            value_size: 0,
            data_entry_position: 0,
            key,
        }
    }
    pub(crate)  fn data_entry_position(&self) -> u64 {
        self.data_entry_position
    }

    pub(crate)  fn is_deleted(&self) -> bool {
        self.level < 0 && self.value_size == 0 && self.data_entry_position == 0
    }

    pub(crate)  fn key_size(&self) -> u64 {
        self.key_size
    }
    pub(crate)  fn value_size(&self) -> u64 {
        self.value_size
    }
    pub(crate)  fn level(&self) -> i64 {
        self.level
    }
    pub(crate)  fn key(&self) -> Vec<u8> {
        self.key.to_owned()
    }

}

impl Encoder for HintEntry {
    fn encode(&self) -> Vec<u8> {
        let mut buf = vec![];
        buf.extend_from_slice(&self.level.to_be_bytes());
        buf.extend_from_slice(&self.key_size.to_be_bytes());
        buf.extend_from_slice(&self.value_size.to_be_bytes());
        buf.extend_from_slice(&self.data_entry_position.to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf
    }
}

impl Decoder for HintEntry {
    fn decode<R: Read>(rdr: &mut R) -> Result<Self> where Self: Sized {
        let mut out = Self {
            level: 0,
            key_size: 0,
            value_size: 0,
            data_entry_position: 0,
            key: vec![],
        };

        let mut raw_level_bytes = [0_u8; 8];
        let mut raw_key_size_bytes = [0_u8; 8];
        let mut raw_value_size_bytes = [0_u8; 8];
        let mut raw_data_entry_pos_size_bytes = [0_u8; 8];

        rdr.read_exact(&mut raw_level_bytes)?;
        rdr.read_exact(&mut raw_key_size_bytes)?;
        rdr.read_exact(&mut raw_value_size_bytes)?;
        rdr.read_exact(&mut raw_data_entry_pos_size_bytes)?;

        out.level = i64::from_be_bytes(raw_level_bytes);
        out.key_size = u64::from_be_bytes(raw_key_size_bytes);
        out.value_size = u64::from_be_bytes(raw_value_size_bytes);
        out.data_entry_position = u64::from_be_bytes(raw_data_entry_pos_size_bytes);

        let mut raw_key_bytes = vec![0_u8; out.key_size as usize];
        rdr.read_exact(&mut raw_key_bytes);
        out.key = raw_key_bytes;

        Ok(out)

    }
}



#[cfg(test)]
mod tests {
    use crate::schema::{DataEntry, Encoder, Decoder};
    use std::io::{Cursor};

    #[test]
    fn decode_encode_test() {
        let rec = DataEntry::new(0,vec![2, 2, 3, 54, 12], vec![32, 4, 1, 32, 65, 78]);
        let e = rec.encode();
        let d = DataEntry::decode(&mut Cursor::new(e)).unwrap();
        println!("{:#?}", d);
        println!("{}", d.check_crc())
    }
}
