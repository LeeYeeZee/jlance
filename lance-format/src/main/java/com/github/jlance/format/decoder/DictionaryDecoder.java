// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Dictionary} encoding into a flattened Arrow vector.
 *
 * <p>Lance stores dictionary-encoded data with an inline items array (dictionary values) and an
 * indices array. This decoder expands the indices into the actual value vector.
 */
public class DictionaryDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var dict = encoding.getDictionary();

    // Decode dictionary values
    ArrayEncoding itemsEncoding = dict.getItems();
    ArrayDecoder itemsDecoder = PageDecoder.createDecoder(itemsEncoding);
    FieldVector dictValues =
        itemsDecoder.decode(
            itemsEncoding, (int) dict.getNumDictionaryItems(), store, field, allocator);

    // Decode indices
    ArrayEncoding indicesEncoding = dict.getIndices();
    ArrayDecoder indicesDecoder = PageDecoder.createDecoder(indicesEncoding);

    // Determine physical bit width of indices from the encoding tree.
    int indexBits = extractIndexBitsPerValue(indicesEncoding);
    Field indexField = createIndexField(indexBits);
    FieldVector indicesVec =
        indicesDecoder.decode(indicesEncoding, numRows, store, indexField, allocator);

    // Create result vector (same type as dict values)
    FieldVector result = field.createVector(allocator);
    result.allocateNew();

    TransferPair transferPair = dictValues.makeTransferPair(result);

    for (int i = 0; i < numRows; i++) {
      if (indicesVec.isNull(i)) {
        result.setNull(i);
      } else {
        int idx = readIndex(indicesVec, i);
        transferPair.copyValueSafe(idx, i);
      }
    }

    result.setValueCount(numRows);
    dictValues.close();
    indicesVec.close();
    return result;
  }

  /**
   * Extracts the physical bits-per-value from a dictionary indices encoding tree.
   *
   * <p>Walks through Nullable wrappers to find the underlying Flat encoding.
   */
  private static int extractIndexBitsPerValue(ArrayEncoding encoding) {
    if (encoding.hasFlat()) {
      return (int) encoding.getFlat().getBitsPerValue();
    }
    if (encoding.hasNullable()) {
      var nullable = encoding.getNullable();
      if (nullable.hasNoNulls()) {
        return extractIndexBitsPerValue(nullable.getNoNulls().getValues());
      }
      if (nullable.hasSomeNulls()) {
        return extractIndexBitsPerValue(nullable.getSomeNulls().getValues());
      }
    }
    // Fallback: assume 32-bit indices
    return 32;
  }

  private static Field createIndexField(int bitsPerValue) {
    if (bitsPerValue <= 8) {
      return new Field("indices", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(8, true)), null);
    } else if (bitsPerValue <= 16) {
      return new Field("indices", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(16, true)), null);
    } else if (bitsPerValue <= 32) {
      return new Field("indices", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true)), null);
    } else {
      return new Field("indices", FieldType.nullable(new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true)), null);
    }
  }

  private static int readIndex(FieldVector indexVec, int i) {
    if (indexVec instanceof IntVector) {
      return ((IntVector) indexVec).get(i);
    }
    if (indexVec instanceof BigIntVector) {
      return (int) ((BigIntVector) indexVec).get(i);
    }
    if (indexVec instanceof SmallIntVector) {
      return ((SmallIntVector) indexVec).get(i);
    }
    if (indexVec instanceof TinyIntVector) {
      return ((TinyIntVector) indexVec).get(i);
    }
    if (indexVec instanceof UInt1Vector) {
      return ((UInt1Vector) indexVec).get(i) & 0xFF;
    }
    if (indexVec instanceof UInt2Vector) {
      return ((UInt2Vector) indexVec).get(i);
    }
    if (indexVec instanceof UInt4Vector) {
      return ((UInt4Vector) indexVec).get(i);
    }
    throw new UnsupportedOperationException(
        "Unsupported index vector type: " + indexVec.getClass().getName());
  }
}
