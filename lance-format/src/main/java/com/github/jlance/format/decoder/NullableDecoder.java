package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Nullable} encoding.
 *
 * <p>Wraps an inner decoder and applies nullability information.
 */
public class NullableDecoder implements ArrayDecoder {

  private final ArrayDecoder inner;

  public NullableDecoder(ArrayDecoder inner) {
    this.inner = inner;
  }

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var nullable = encoding.getNullable();
    if (nullable.hasNoNulls()) {
      return inner.decode(nullable.getNoNulls().getValues(), numRows, store, field, allocator);
    }
    if (nullable.hasAllNulls()) {
      FieldVector vector = field.createVector(allocator);
      vector.allocateNew();
      for (int i = 0; i < numRows; i++) {
        vector.setNull(i);
      }
      vector.setValueCount(numRows);
      return vector;
    }
    if (nullable.hasSomeNulls()) {
      var someNulls = nullable.getSomeNulls();
      // Decode values first
      FieldVector vector = inner.decode(someNulls.getValues(), numRows, store, field, allocator);

      // Read validity bitmap
      int validityBufferIndex = someNulls.getValidity().getFlat().getBuffer().getBufferIndex();
      byte[] validityBytes = store.getBuffer(validityBufferIndex);

      // Apply nulls: bit = 0 means null, bit = 1 means valid (LSB order)
      for (int i = 0; i < numRows; i++) {
        int byteIdx = i / 8;
        int bitIdx = i % 8;
        boolean isValid = (validityBytes[byteIdx] & (1 << bitIdx)) != 0;
        if (!isValid) {
          vector.setNull(i);
        }
      }
      vector.setValueCount(numRows);
      return vector;
    }
    throw new IllegalArgumentException("Unknown nullability in Nullable encoding");
  }
}
