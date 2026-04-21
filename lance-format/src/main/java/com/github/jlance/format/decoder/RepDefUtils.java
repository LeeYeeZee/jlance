package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import lance.encodings21.EncodingsV21.CompressiveEncoding;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Utilities for decoding Lance V2.1+ repetition and definition levels.
 *
 * <p>Rep/def levels encode structural information (nullability, list boundaries)
 * that was previously embedded in the V2.0 encoding tree as {@code Nullable} /
 * {@code List} nodes.
 */
public final class RepDefUtils {

  private RepDefUtils() {}

  /**
   * Decodes a 1-bit-per-value definition bitmap into a boolean array.
   *
   * @param bitmap raw bitmap bytes (little-endian bit order: bit 0 = first value)
   * @param numValues number of values to decode
   * @return boolean array where {@code true} = valid, {@code false} = null
   */
  public static boolean[] decodeDefBitmap(byte[] bitmap, int numValues) {
    boolean[] result = new boolean[numValues];
    for (int i = 0; i < numValues; i++) {
      int byteIdx = i / 8;
      int bitIdx = i % 8;
      result[i] = (bitmap[byteIdx] & (1 << bitIdx)) != 0;
    }
    return result;
  }

  /**
   * Reads a definition-level buffer from the page buffer store.
   *
   * <p>If a compressive encoding is provided, the buffer is decompressed first.
   * Otherwise the next raw page buffer is returned.
   *
   * @param store the page buffer store
   * @param defCompression optional compressive encoding for the def buffer
   * @param numDefValues number of def values (used for Constant encoding)
   * @param allocator memory allocator for temporary buffers
   * @return raw def buffer bytes
   */
  public static byte[] readDefBuffer(
      PageBufferStore store,
      CompressiveEncoding defCompression,
      int numDefValues,
      BufferAllocator allocator) {
    if (defCompression != null
        && defCompression.getCompressionCase()
            != CompressiveEncoding.CompressionCase.COMPRESSION_NOT_SET) {
      ByteBuffer decoded =
          CompressiveEncodingDecoders.decode(defCompression, numDefValues, store, allocator);
      byte[] result = new byte[decoded.remaining()];
      decoded.get(result);
      return result;
    }
    return store.takeNextBuffer();
  }
}
