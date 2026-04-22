// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Flat} encoding into an Arrow vector.
 *
 * <p>Currently supports primitive fixed-width types where bits_per_value is a multiple of 8.
 */
public class FlatDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var flat = encoding.getFlat();
    int bitsPerValue = (int) flat.getBitsPerValue();
    int bufferIndex = flat.getBuffer().getBufferIndex();
    byte[] data = store.getBuffer(bufferIndex);
    if (flat.hasCompression()) {
      String scheme = flat.getCompression().getScheme();
      data = CompressionUtils.maybeDecompress(data, scheme);
    }

    ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
    return FixedWidthVectorBuilder.build(field, numRows, buf, bitsPerValue, allocator);
  }
}
