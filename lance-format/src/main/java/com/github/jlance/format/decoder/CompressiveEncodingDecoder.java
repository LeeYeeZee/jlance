package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lance.encodings21.EncodingsV21.CompressiveEncoding;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Decodes a Lance V2.1+ {@link CompressiveEncoding} tree into decompressed data blocks.
 *
 * <p>A {@code CompressiveEncoding} forms a tree whose leaves are buffers stored in the page.
 * This decoder walks the tree, reading buffers from the {@link PageBufferStore} and applying
 * decompression to produce raw byte data.
 */
public interface CompressiveEncodingDecoder {

  /**
   * Decodes the compressive encoding tree into a raw byte buffer.
   *
   * @param encoding the compressive encoding tree root
   * @param numValues number of values expected in the output
   * @param store buffer store containing all page buffers
   * @param allocator memory allocator for temporary buffers
   * @return the decompressed raw bytes (little-endian fixed-width values)
   */
  ByteBuffer decode(
      CompressiveEncoding encoding,
      int numValues,
      PageBufferStore store,
      BufferAllocator allocator);

  /**
   * Helper to read a flat little-endian value from a byte buffer.
   */
  static long readLittleEndianValue(byte[] data, int offset, int bytesPerValue) {
    long value = 0;
    for (int i = 0; i < bytesPerValue; i++) {
      value |= ((long) (data[offset + i] & 0xFF)) << (i * 8);
    }
    return value;
  }

  /**
   * Helper to create a little-endian ByteBuffer from raw bytes.
   */
  static ByteBuffer wrapLittleEndian(byte[] data) {
    return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
  }
}
