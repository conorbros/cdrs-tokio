use cassandra_protocol::compression::Compression;
use cassandra_protocol::envelope::Version;
use cassandra_protocol::frame::decoder::{
    FrameDecode, LegacyFrameDecoder, Lz4FrameDecoder, UncompressedFrameDecoder,
};
use cassandra_protocol::frame::encoder::{
    FrameEncode, LegacyFrameEncoder, Lz4FrameEncoder, UncompressedFrameEncoder,
};

/// A factory for frame encoder/decoder.
pub trait FrameEncodingFactory {
    /// Creates a new frame encoder based on given protocol settings.
    fn create_encoder(
        &self,
        version: Version,
        compression: Compression,
    ) -> Box<dyn FrameEncode + Send + Sync>;

    /// Creates a new frame decoder based on given protocol settings.
    fn create_decoder(
        &self,
        version: Version,
        compression: Compression,
    ) -> Box<dyn FrameDecode + Send + Sync>;
}

/// Frame encoding factor based on protocol settings.
#[derive(Copy, Clone, Debug, Default, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ProtocolFrameEncodingFactory;

impl FrameEncodingFactory for ProtocolFrameEncodingFactory {
    fn create_encoder(
        &self,
        version: Version,
        compression: Compression,
    ) -> Box<dyn FrameEncode + Send + Sync> {
        if version >= Version::V5 {
            match compression {
                Compression::Lz4 => Box::new(Lz4FrameEncoder::default()),
                // >= v5 supports only lz4 => fall back to uncompressed
                _ => Box::new(UncompressedFrameEncoder::default()),
            }
        } else {
            Box::new(LegacyFrameEncoder::default())
        }
    }

    fn create_decoder(
        &self,
        version: Version,
        compression: Compression,
    ) -> Box<dyn FrameDecode + Send + Sync> {
        if version >= Version::V5 {
            match compression {
                Compression::Lz4 => Box::new(Lz4FrameDecoder::default()),
                // >= v5 supports only lz4 => fall back to uncompressed
                _ => Box::new(UncompressedFrameDecoder::default()),
            }
        } else {
            Box::new(LegacyFrameDecoder::default())
        }
    }
}
