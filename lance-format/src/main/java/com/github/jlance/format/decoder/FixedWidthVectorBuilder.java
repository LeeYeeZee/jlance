// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Builds an Arrow {@link FieldVector} from a little-endian byte buffer of fixed-width values.
 *
 * <p>This class extracts the vector-population logic shared between V2.0 {@link FlatDecoder}
 * and V2.1+ layout decoders (e.g. {@link FullZipLayoutDecoder}).
 */
public final class FixedWidthVectorBuilder {

  private FixedWidthVectorBuilder() {}

  /**
   * Creates and populates a fixed-width Arrow vector from raw little-endian bytes.
   *
   * @param field the Arrow field descriptor
   * @param numRows number of rows to populate
   * @param buffer little-endian byte buffer containing the raw values
   * @param bitsPerValue number of bits per value (must be a multiple of 8, except 1 for booleans)
   * @param allocator memory allocator for the vector
   * @return the populated vector
   */
  public static FieldVector build(
      Field field, int numRows, ByteBuffer buffer, int bitsPerValue, BufferAllocator allocator) {
    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (bitsPerValue == 1) {
      if (vector instanceof BitVector) {
        fillBitVector((BitVector) vector, numRows, buffer);
      } else {
        throw new UnsupportedOperationException(
            "bits=1 only supports BitVector, got: " + vector.getClass().getName());
      }
      vector.setValueCount(numRows);
      return vector;
    }

    int logicalBytesPerValue = getLogicalBytesPerValue(vector);
    fillFixedWidthVector(vector, numRows, buffer, logicalBytesPerValue);

    vector.setValueCount(numRows);
    return vector;
  }

  /**
   * Creates and populates a fixed-width Arrow vector from raw little-endian bytes,
   * with a per-row validity bitmap.
   *
   * <p>The {@code valueBuffer} contains <em>only</em> valid (non-null) values, in row order.
   * Values for null rows are skipped. This is the standard V2.1+ layout where definition
   * levels are stored separately from the value data.
   *
   * @param field the Arrow field descriptor
   * @param numRows number of rows to populate
   * @param valueBuffer little-endian byte buffer containing only valid raw values
   * @param bitsPerValue number of bits per value
   * @param validity boolean array where {@code true} = valid, {@code false} = null
   * @param allocator memory allocator for the vector
   * @return the populated vector
   */
  public static FieldVector buildWithValidity(
      Field field,
      int numRows,
      ByteBuffer valueBuffer,
      int bitsPerValue,
      boolean[] validity,
      BufferAllocator allocator) {
    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (bitsPerValue == 1) {
      // Expand value buffer to include placeholders for nulls
      byte[] expanded = new byte[(numRows + 7) / 8];
      int srcByteIdx = 0;
      int srcBitIdx = 0;
      for (int i = 0; i < numRows; i++) {
        if (validity[i]) {
          boolean value = (valueBuffer.get(srcByteIdx) & (1 << srcBitIdx)) != 0;
          if (value) {
            expanded[i / 8] |= (1 << (i % 8));
          }
          srcBitIdx++;
          if (srcBitIdx == 8) {
            srcBitIdx = 0;
            srcByteIdx++;
          }
        }
      }
      fillBitVector((BitVector) vector, numRows,
          ByteBuffer.wrap(expanded).order(ByteOrder.LITTLE_ENDIAN));
      vector.setValueCount(numRows);
      // Apply nulls
      for (int i = 0; i < numRows; i++) {
        if (!validity[i]) {
          vector.setNull(i);
        }
      }
      return vector;
    }

    int logicalBytesPerValue = getLogicalBytesPerValue(vector);

    // Expand value buffer to include zero placeholders for null rows
    byte[] expanded = new byte[numRows * logicalBytesPerValue];
    for (int i = 0; i < numRows; i++) {
      if (validity[i]) {
        valueBuffer.get(expanded, i * logicalBytesPerValue, logicalBytesPerValue);
      }
      // else: leave as zeros
    }

    fillFixedWidthVector(vector, numRows,
        ByteBuffer.wrap(expanded).order(ByteOrder.LITTLE_ENDIAN), logicalBytesPerValue);

    // Apply nulls
    for (int i = 0; i < numRows; i++) {
      if (!validity[i]) {
        vector.setNull(i);
      }
    }

    vector.setValueCount(numRows);
    return vector;
  }

  private static void fillBitVector(BitVector vector, int numRows, ByteBuffer buffer) {
    byte[] data = new byte[buffer.remaining()];
    buffer.get(data);
    for (int i = 0; i < numRows; i++) {
      int byteIdx = i / 8;
      int bitIdx = i % 8;
      boolean value = (data[byteIdx] & (1 << bitIdx)) != 0;
      vector.set(i, value ? 1 : 0);
    }
  }

  @SuppressWarnings("checkstyle:methodlength")
  private static void fillFixedWidthVector(
      FieldVector vector, int numRows, ByteBuffer buffer, int bytesPerValue) {
    if (vector instanceof TinyIntVector tinyIntVec) {
      for (int i = 0; i < numRows; i++) {
        tinyIntVec.set(i, (byte) readValue(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof SmallIntVector smallIntVec) {
      for (int i = 0; i < numRows; i++) {
        smallIntVec.set(i, (short) readValue(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof IntVector intVec) {
      for (int i = 0; i < numRows; i++) {
        intVec.set(i, (int) readValue(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof BigIntVector bigIntVec) {
      for (int i = 0; i < numRows; i++) {
        bigIntVec.set(i, buffer.getLong(i * bytesPerValue));
      }
    } else if (vector instanceof UInt1Vector uInt1Vec) {
      for (int i = 0; i < numRows; i++) {
        uInt1Vec.set(i, (byte) (readValue(buffer, i, bytesPerValue) & 0xFF));
      }
    } else if (vector instanceof UInt2Vector uInt2Vec) {
      for (int i = 0; i < numRows; i++) {
        uInt2Vec.set(i, (char) (readValue(buffer, i, bytesPerValue) & 0xFFFF));
      }
    } else if (vector instanceof UInt4Vector uInt4Vec) {
      for (int i = 0; i < numRows; i++) {
        uInt4Vec.set(i, (int) (readValue(buffer, i, bytesPerValue) & 0xFFFFFFFFL));
      }
    } else if (vector instanceof UInt8Vector uInt8Vec) {
      for (int i = 0; i < numRows; i++) {
        uInt8Vec.set(i, buffer.getLong(i * bytesPerValue));
      }
    } else if (vector instanceof Float2Vector float2Vec) {
      for (int i = 0; i < numRows; i++) {
        float2Vec.set(i, buffer.getShort(i * bytesPerValue));
      }
    } else if (vector instanceof Float4Vector float4Vec) {
      for (int i = 0; i < numRows; i++) {
        float4Vec.set(i, buffer.getFloat(i * bytesPerValue));
      }
    } else if (vector instanceof Float8Vector float8Vec) {
      for (int i = 0; i < numRows; i++) {
        float8Vec.set(i, buffer.getDouble(i * bytesPerValue));
      }
    } else if (vector instanceof DateDayVector dateDayVec) {
      for (int i = 0; i < numRows; i++) {
        dateDayVec.set(i, (int) readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof DateMilliVector dateMilliVec) {
      for (int i = 0; i < numRows; i++) {
        dateMilliVec.set(i, readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof TimeStampVector timeStampVec) {
      for (int i = 0; i < numRows; i++) {
        timeStampVec.set(i, readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof TimeSecVector timeSecVec) {
      for (int i = 0; i < numRows; i++) {
        timeSecVec.set(i, (int) readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof TimeMilliVector timeMilliVec) {
      for (int i = 0; i < numRows; i++) {
        timeMilliVec.set(i, (int) readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof TimeMicroVector timeMicroVec) {
      for (int i = 0; i < numRows; i++) {
        timeMicroVec.set(i, readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof TimeNanoVector timeNanoVec) {
      for (int i = 0; i < numRows; i++) {
        timeNanoVec.set(i, readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof DurationVector durationVec) {
      for (int i = 0; i < numRows; i++) {
        durationVec.set(i, readValueAsLong(buffer, i, bytesPerValue));
      }
    } else if (vector instanceof DecimalVector decimalVec) {
      org.apache.arrow.memory.ArrowBuf dataBuf = decimalVec.getDataBuffer();
      for (int i = 0; i < numRows; i++) {
        for (int b = 0; b < bytesPerValue; b++) {
          dataBuf.setByte(i * bytesPerValue + b, buffer.get(i * bytesPerValue + b));
        }
        decimalVec.setIndexDefined(i);
      }
    } else if (vector instanceof Decimal256Vector decimal256Vec) {
      org.apache.arrow.memory.ArrowBuf dataBuf = decimal256Vec.getDataBuffer();
      for (int i = 0; i < numRows; i++) {
        for (int b = 0; b < bytesPerValue; b++) {
          dataBuf.setByte(i * bytesPerValue + b, buffer.get(i * bytesPerValue + b));
        }
        decimal256Vec.setIndexDefined(i);
      }
    } else if (vector instanceof FixedSizeBinaryVector fsbVec) {
      for (int i = 0; i < numRows; i++) {
        byte[] value = new byte[bytesPerValue];
        buffer.get(i * bytesPerValue, value);
        fsbVec.set(i, value);
      }
    } else {
      throw new UnsupportedOperationException(
          "FixedWidthVectorBuilder does not support vector type: " + vector.getClass().getName());
    }
  }

  private static long readValue(ByteBuffer buffer, int index, int bytesPerValue) {
    int offset = index * bytesPerValue;
    return switch (bytesPerValue) {
      case 1 -> buffer.get(offset);
      case 2 -> buffer.getShort(offset);
      case 4 -> buffer.getInt(offset);
      case 8 -> buffer.getLong(offset);
      default -> throw new UnsupportedOperationException(
          "Unsupported bytesPerValue: " + bytesPerValue);
    };
  }

  /**
   * Expands a vector containing only valid (non-null) values into a full-sized vector,
   * using a validity bitmap to determine where nulls belong.
   *
   * <p>This is the vector-level equivalent of {@link #buildWithValidity} — instead of
   * starting from raw bytes, it starts from an already-built vector of valid values.
   *
   * @param validValues vector containing only the valid values, in row order
   * @param validity boolean array where {@code true} = valid, {@code false} = null
   * @param numRows total number of rows in the output vector
   * @param field the Arrow field descriptor for the output vector
   * @param allocator memory allocator
   * @return a new vector of length {@code numRows} with nulls applied
   */
  public static FieldVector expandWithValidity(
      FieldVector validValues,
      boolean[] validity,
      int numRows,
      Field field,
      BufferAllocator allocator) {
    FieldVector result = field.createVector(allocator);
    result.allocateNew();
    org.apache.arrow.vector.util.TransferPair transferPair = validValues.makeTransferPair(result);
    int validIdx = 0;
    for (int i = 0; i < numRows; i++) {
      if (validity[i]) {
        transferPair.copyValueSafe(validIdx, i);
        validIdx++;
      } else {
        result.setNull(i);
      }
    }
    result.setValueCount(numRows);
    validValues.close();
    return result;
  }

  private static long readValueAsLong(ByteBuffer buffer, int index, int bytesPerValue) {
    return switch (bytesPerValue) {
      case 1 -> buffer.get(index);
      case 2 -> buffer.getShort(index * 2);
      case 4 -> buffer.getInt(index * 4);
      case 8 -> buffer.getLong(index * 8);
      default -> throw new UnsupportedOperationException(
          "Unsupported bytesPerValue for temporal: " + bytesPerValue);
    };
  }

  /**
   * Returns the logical byte width for the given Arrow vector type.
   *
   * <p>This is the width dictated by the Arrow schema (e.g. 4 bytes for Int32)
   * and may differ from the physical storage width reported by the encoding.
   */
  static int getLogicalBytesPerValue(FieldVector vector) {
    if (vector instanceof TinyIntVector || vector instanceof UInt1Vector) {
      return 1;
    } else if (vector instanceof SmallIntVector
        || vector instanceof UInt2Vector
        || vector instanceof Float2Vector) {
      return 2;
    } else if (vector instanceof IntVector
        || vector instanceof UInt4Vector
        || vector instanceof Float4Vector
        || vector instanceof DateDayVector
        || vector instanceof org.apache.arrow.vector.TimeSecVector
        || vector instanceof org.apache.arrow.vector.TimeMilliVector) {
      return 4;
    } else if (vector instanceof BigIntVector
        || vector instanceof UInt8Vector
        || vector instanceof Float8Vector
        || vector instanceof DateMilliVector
        || vector instanceof TimeStampVector
        || vector instanceof TimeMicroVector
        || vector instanceof TimeNanoVector
        || vector instanceof DurationVector) {
      return 8;
    } else if (vector instanceof DecimalVector) {
      return 16;
    } else if (vector instanceof Decimal256Vector) {
      return 32;
    } else if (vector instanceof FixedSizeBinaryVector fsbVec) {
      return fsbVec.getByteWidth();
    } else {
      throw new UnsupportedOperationException(
          "Cannot determine logical bytes per value for vector type: " + vector.getClass().getName());
    }
  }
}
