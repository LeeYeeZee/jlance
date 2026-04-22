// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Binary} encoding into an Arrow {@link
 * VarCharVector} or {@link VarBinaryVector}.
 *
 * <p>Lance stores binary data with two arrays:
 *
 * <ul>
 *   <li>{@code indices} – end offsets for each value (one per row). Nulls are represented by
 *       adding {@code null_adjustment} to the offset.
 *   <li>{@code bytes} – the raw concatenated byte data.
 * </ul>
 */
public class BinaryDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var binary = encoding.getBinary();
    long nullAdjustment = binary.getNullAdjustment();

    // Decode indices (end offsets, one per row)
    long[] lanceIndices = decodeFlatUint64(binary.getIndices(), numRows, store);

    // Decode raw bytes
    byte[] bytesData = decodeFlatBytes(binary.getBytes(), store);

    // Convert Lance indices to Arrow cumulative offsets
    long[] arrowOffsets = new long[numRows + 1];
    arrowOffsets[0] = 0;
    long last = 0;
    long prev = normalize(lanceIndices[0], nullAdjustment).value;
    arrowOffsets[1] = prev;
    last = prev;

    for (int i = 1; i < numRows; i++) {
      NormResult norm = normalize(lanceIndices[i], nullAdjustment);
      long next = norm.value - prev + last;
      arrowOffsets[i + 1] = next;
      prev = norm.value;
      last = next;
    }

    FieldVector vector = field.createVector(allocator);
    if (vector instanceof VarCharVector) {
      VarCharVector vec = (VarCharVector) vector;
      vec.allocateNew(numRows);
      for (int i = 0; i < numRows; i++) {
        if (lanceIndices[i] >= nullAdjustment) {
          vec.setNull(i);
        } else {
          int start = (int) arrowOffsets[i];
          int end = (int) arrowOffsets[i + 1];
          byte[] value = java.util.Arrays.copyOfRange(bytesData, start, end);
          vec.setSafe(i, value);
        }
      }
      vec.setValueCount(numRows);
      return vec;
    }
    if (vector instanceof VarBinaryVector) {
      VarBinaryVector vec = (VarBinaryVector) vector;
      vec.allocateNew(numRows);
      for (int i = 0; i < numRows; i++) {
        if (lanceIndices[i] >= nullAdjustment) {
          vec.setNull(i);
        } else {
          int start = (int) arrowOffsets[i];
          int end = (int) arrowOffsets[i + 1];
          byte[] value = java.util.Arrays.copyOfRange(bytesData, start, end);
          vec.setSafe(i, value);
        }
      }
      vec.setValueCount(numRows);
      return vec;
    }
    if (vector instanceof LargeVarCharVector) {
      LargeVarCharVector vec = (LargeVarCharVector) vector;
      vec.allocateNew(numRows);
      for (int i = 0; i < numRows; i++) {
        if (lanceIndices[i] >= nullAdjustment) {
          vec.setNull(i);
        } else {
          int start = (int) arrowOffsets[i];
          int end = (int) arrowOffsets[i + 1];
          byte[] value = java.util.Arrays.copyOfRange(bytesData, start, end);
          vec.setSafe(i, value);
        }
      }
      vec.setValueCount(numRows);
      return vec;
    }
    if (vector instanceof LargeVarBinaryVector) {
      LargeVarBinaryVector vec = (LargeVarBinaryVector) vector;
      vec.allocateNew(numRows);
      for (int i = 0; i < numRows; i++) {
        if (lanceIndices[i] >= nullAdjustment) {
          vec.setNull(i);
        } else {
          int start = (int) arrowOffsets[i];
          int end = (int) arrowOffsets[i + 1];
          byte[] value = java.util.Arrays.copyOfRange(bytesData, start, end);
          vec.setSafe(i, value);
        }
      }
      vec.setValueCount(numRows);
      return vec;
    }
    throw new UnsupportedOperationException(
        "BinaryDecoder does not yet support vector type: " + vector.getClass().getName());
  }

  private static long[] decodeFlatUint64(ArrayEncoding encoding, int numRows, PageBufferStore store) {
    if (encoding.hasNullable()) {
      var nullable = encoding.getNullable();
      if (nullable.hasNoNulls()) {
        return decodeFlatUint64(nullable.getNoNulls().getValues(), numRows, store);
      }
      if (nullable.hasAllNulls()) {
        return new long[numRows];
      }
      if (nullable.hasSomeNulls()) {
        var someNulls = nullable.getSomeNulls();
        long[] values = decodeFlatUint64(someNulls.getValues(), numRows, store);
        int validityBufferIndex =
            someNulls.getValidity().getFlat().getBuffer().getBufferIndex();
        byte[] validity = store.getBuffer(validityBufferIndex);
        for (int i = 0; i < numRows; i++) {
          int byteIdx = i / 8;
          int bitIdx = i % 8;
          boolean isValid = (validity[byteIdx] & (1 << bitIdx)) != 0;
          if (!isValid) {
            values[i] = 0;
          }
        }
        return values;
      }
    }
    if (encoding.hasFlat()) {
      var flat = encoding.getFlat();
      int bitsPerValue = (int) flat.getBitsPerValue();
      if (bitsPerValue != 64) {
        throw new UnsupportedOperationException("Expected 64-bit indices, got " + bitsPerValue);
      }
      int bufferIndex = flat.getBuffer().getBufferIndex();
      byte[] data = store.getBuffer(bufferIndex);
      if (flat.hasCompression()) {
        String scheme = flat.getCompression().getScheme();
        data = CompressionUtils.maybeDecompress(data, scheme);
      }
      ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
      long[] result = new long[numRows];
      for (int i = 0; i < numRows; i++) {
        result[i] = buf.getLong(i * 8);
      }
      return result;
    }
    throw new UnsupportedOperationException("Unsupported encoding for binary indices");
  }

  private static byte[] decodeFlatBytes(ArrayEncoding encoding, PageBufferStore store) {
    if (encoding.hasNullable()) {
      throw new UnsupportedOperationException("Nullable bytes not supported in BinaryDecoder");
    }
    if (encoding.hasFlat()) {
      var flat = encoding.getFlat();
      int bitsPerValue = (int) flat.getBitsPerValue();
      if (bitsPerValue != 8) {
        throw new UnsupportedOperationException("Expected 8-bit bytes, got " + bitsPerValue);
      }
      int bufferIndex = flat.getBuffer().getBufferIndex();
      byte[] data = store.getBuffer(bufferIndex);
      if (flat.hasCompression()) {
        String scheme = flat.getCompression().getScheme();
        data = CompressionUtils.maybeDecompress(data, scheme);
      }
      return data;
    }
    throw new UnsupportedOperationException("Unsupported encoding for binary bytes");
  }

  private static NormResult normalize(long val, long nullAdjustment) {
    if (val >= nullAdjustment) {
      return new NormResult(false, val - nullAdjustment);
    }
    return new NormResult(true, val);
  }

  private record NormResult(boolean valid, long value) {}
}
