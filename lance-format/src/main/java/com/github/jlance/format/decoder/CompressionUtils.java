package com.github.jlance.format.decoder;

import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decompresses Lance page buffers according to the compression scheme specified in the
 * {@link lance.encodings.EncodingsV20.Compression} protobuf.
 */
public final class CompressionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionUtils.class);

  private CompressionUtils() {}

  /**
   * Decompresses the given data if a supported compression scheme is provided.
   *
   * @param data the raw (possibly compressed) buffer data
   * @param scheme the compression scheme name (e.g. "zstd", "lz4", or empty/null for none)
   * @return the uncompressed data, or the original data if no compression is specified
   */
  public static byte[] maybeDecompress(byte[] data, String scheme) {
    if (scheme == null || scheme.isEmpty() || "none".equalsIgnoreCase(scheme)) {
      return data;
    }

    if ("zstd".equalsIgnoreCase(scheme)) {
      // Lance stores compressed buffers with an 8-byte little-endian prefix
      // containing the decompressed size, followed by the Zstd frame.
      if (data.length < 8) {
        throw new IllegalArgumentException(
            "Zstd compressed buffer too small to contain size prefix: " + data.length);
      }
      long decompressedSize =
          ((long) (data[0] & 0xFF))
              | ((long) (data[1] & 0xFF) << 8)
              | ((long) (data[2] & 0xFF) << 16)
              | ((long) (data[3] & 0xFF) << 24)
              | ((long) (data[4] & 0xFF) << 32)
              | ((long) (data[5] & 0xFF) << 40)
              | ((long) (data[6] & 0xFF) << 48)
              | ((long) (data[7] & 0xFF) << 56);
      if (decompressedSize < 0 || decompressedSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "Zstd decompressed size too large: " + decompressedSize);
      }
      byte[] frame = java.util.Arrays.copyOfRange(data, 8, data.length);
      byte[] result = new byte[(int) decompressedSize];
      long actualSize = Zstd.decompress(result, frame);
      if (Zstd.isError(actualSize)) {
        throw new IllegalArgumentException(
            "Zstd decompression failed: " + Zstd.getErrorName(actualSize));
      }
      return result;
    }

    throw new UnsupportedOperationException(
        "Unsupported compression scheme: " + scheme);
  }

  /**
   * Decompresses the given data using a V2.1 {@link CompressionScheme}.
   *
   * @param data the raw (possibly compressed) buffer data
   * @param scheme the V2.1 compression scheme enum
   * @return the uncompressed data, or the original data if no compression is specified
   */
  public static byte[] maybeDecompress(
      byte[] data, lance.encodings21.EncodingsV21.CompressionScheme scheme) {
    if (scheme == null
        || scheme == lance.encodings21.EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_UNSPECIFIED) {
      return data;
    }

    if (scheme == lance.encodings21.EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD) {
      return decompressZstd(data);
    }

    if (scheme == lance.encodings21.EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_LZ4) {
      return decompressLz4(data);
    }

    throw new UnsupportedOperationException("Unsupported compression scheme: " + scheme);
  }

  private static byte[] decompressZstd(byte[] data) {
    if (data.length < 8) {
      throw new IllegalArgumentException(
          "Zstd compressed buffer too small to contain size prefix: " + data.length);
    }
    long decompressedSize =
        ((long) (data[0] & 0xFF))
            | ((long) (data[1] & 0xFF) << 8)
            | ((long) (data[2] & 0xFF) << 16)
            | ((long) (data[3] & 0xFF) << 24)
            | ((long) (data[4] & 0xFF) << 32)
            | ((long) (data[5] & 0xFF) << 40)
            | ((long) (data[6] & 0xFF) << 48)
            | ((long) (data[7] & 0xFF) << 56);
    if (decompressedSize < 0 || decompressedSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Zstd decompressed size too large: " + decompressedSize);
    }
    byte[] frame = java.util.Arrays.copyOfRange(data, 8, data.length);
    byte[] result = new byte[(int) decompressedSize];
    long actualSize = Zstd.decompress(result, frame);
    if (Zstd.isError(actualSize)) {
      throw new IllegalArgumentException(
          "Zstd decompression failed: " + Zstd.getErrorName(actualSize));
    }
    return result;
  }

  private static byte[] decompressLz4(byte[] data) {
    if (data.length < 8) {
      throw new IllegalArgumentException(
          "LZ4 compressed buffer too small to contain size prefix: " + data.length);
    }
    long decompressedSize =
        ((long) (data[0] & 0xFF))
            | ((long) (data[1] & 0xFF) << 8)
            | ((long) (data[2] & 0xFF) << 16)
            | ((long) (data[3] & 0xFF) << 24)
            | ((long) (data[4] & 0xFF) << 32)
            | ((long) (data[5] & 0xFF) << 40)
            | ((long) (data[6] & 0xFF) << 48)
            | ((long) (data[7] & 0xFF) << 56);
    if (decompressedSize < 0 || decompressedSize > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "LZ4 decompressed size too large: " + decompressedSize);
    }
    byte[] frame = java.util.Arrays.copyOfRange(data, 8, data.length);
    byte[] result = new byte[(int) decompressedSize];
    try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(frame);
         net.jpountz.lz4.LZ4FrameInputStream lz4is =
             new net.jpountz.lz4.LZ4FrameInputStream(bais)) {
      int totalRead = 0;
      while (totalRead < result.length) {
        int read = lz4is.read(result, totalRead, result.length - totalRead);
        if (read < 0) {
          throw new IllegalArgumentException(
              "LZ4 decompression produced fewer bytes than expected: "
                  + totalRead + " < " + result.length);
        }
        totalRead += read;
      }
      return result;
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("LZ4 decompression failed", e);
    }
  }
}
