// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Rle} encoding into an Arrow vector.
 *
 * <p>RLE (Run-Length Encoding) stores data as (value, run_length) pairs.
 * The run lengths are stored as u8, and long runs are split into multiple
 * entries of up to 255 values each.
 *
 * <p>Two buffer layouts are supported:
 * <ul>
 *   <li><b>Dual buffer</b>: page buffer 0 = values, page buffer 1 = run lengths</li>
 *   <li><b>Block format</b>: single buffer = [8-byte LE header: values size][values][lengths]</li>
 * </ul>
 *
 * <p>Supported value widths: 8, 16, 32, 64 bits.
 */
public class RleDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var rle = encoding.getRle();
    int bitsPerValue = (int) rle.getBitsPerValue();

    if (bitsPerValue != 8 && bitsPerValue != 16 && bitsPerValue != 32 && bitsPerValue != 64) {
      throw new IllegalArgumentException(
          "RleDecoder only supports 8, 16, 32, or 64 bits per value, got: " + bitsPerValue);
    }

    byte[] valuesBuffer;
    byte[] lengthsBuffer;

    if (store.getBufferCount() == 0) {
      if (numRows == 0) {
        FieldVector emptyVector = field.createVector(allocator);
        emptyVector.setInitialCapacity(0);
        emptyVector.allocateNew();
        emptyVector.setValueCount(0);
        return emptyVector;
      }
      throw new IllegalArgumentException(
          "RleDecoder expects at least one buffer but got none for numRows=" + numRows);
    } else if (store.getBufferCount() == 1) {
      // Block format: [8-byte LE header: values size][values][lengths]
      byte[] block = store.getBuffer(0);
      if (block.length < 8) {
        throw new IllegalArgumentException(
            "RleDecoder block buffer too small: " + block.length + " bytes");
      }
      long valuesSize = ByteBuffer.wrap(block, 0, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
      if (valuesSize < 0 || valuesSize > Integer.MAX_VALUE) {
        throw new IllegalArgumentException(
            "RleDecoder invalid values buffer size: " + valuesSize);
      }
      int valuesSizeInt = (int) valuesSize;
      int lengthsStart = 8 + valuesSizeInt;
      if (block.length < lengthsStart) {
        throw new IllegalArgumentException(
            "RleDecoder block buffer too small for values size " + valuesSizeInt);
      }
      valuesBuffer = new byte[valuesSizeInt];
      System.arraycopy(block, 8, valuesBuffer, 0, valuesSizeInt);
      lengthsBuffer = new byte[block.length - lengthsStart];
      System.arraycopy(block, lengthsStart, lengthsBuffer, 0, lengthsBuffer.length);
    } else {
      // Dual buffer format
      valuesBuffer = store.getBuffer(0);
      lengthsBuffer = store.getBuffer(1);
    }

    int bytesPerValue = bitsPerValue / 8;

    if (valuesBuffer.length % bytesPerValue != 0) {
      throw new IllegalArgumentException(
          "RleDecoder values buffer length " + valuesBuffer.length
              + " is not a multiple of bytes per value " + bytesPerValue);
    }

    int numRuns = valuesBuffer.length / bytesPerValue;
    if (numRuns != lengthsBuffer.length) {
      throw new IllegalArgumentException(
          "RleDecoder inconsistent buffers: " + numRuns + " runs but "
              + lengthsBuffer.length + " length entries");
    }

    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    if (numRows == 0) {
      vector.setValueCount(0);
      return vector;
    }

    try {
      int rowIndex = 0;
      for (int run = 0; run < numRuns && rowIndex < numRows; run++) {
        int length = lengthsBuffer[run] & 0xFF;
        if (length == 0) {
          throw new IllegalArgumentException(
              "RleDecoder encountered zero run length at run index " + run);
        }
        int runEnd = Math.min(rowIndex + length, numRows);

        // Read the value for this run
        int valueOffset = run * bytesPerValue;
        long longValue = readLittleEndianValue(valuesBuffer, valueOffset, bytesPerValue);

        while (rowIndex < runEnd) {
          writeValue(vector, rowIndex, longValue, bitsPerValue);
          rowIndex++;
        }
      }

      if (rowIndex != numRows) {
        throw new IllegalArgumentException(
            "RleDecoder decoded " + rowIndex + " values but expected " + numRows);
      }

      vector.setValueCount(numRows);
      return vector;
    } catch (RuntimeException e) {
      vector.close();
      throw e;
    }
  }

  private static long readLittleEndianValue(byte[] data, int offset, int bytesPerValue) {
    long value = 0;
    for (int i = 0; i < bytesPerValue; i++) {
      value |= ((long) (data[offset + i] & 0xFF)) << (i * 8);
    }
    return value;
  }

  private static void writeValue(FieldVector vector, int index, long value, int bitsPerValue) {
    if (vector instanceof TinyIntVector) {
      ((TinyIntVector) vector).set(index, (byte) value);
    } else if (vector instanceof SmallIntVector) {
      ((SmallIntVector) vector).set(index, (short) value);
    } else if (vector instanceof IntVector) {
      ((IntVector) vector).set(index, (int) value);
    } else if (vector instanceof BigIntVector) {
      ((BigIntVector) vector).set(index, value);
    } else if (vector instanceof UInt1Vector) {
      ((UInt1Vector) vector).set(index, (byte) (value & 0xFF));
    } else if (vector instanceof UInt2Vector) {
      ((UInt2Vector) vector).set(index, (char) (value & 0xFFFF));
    } else if (vector instanceof UInt4Vector) {
      ((UInt4Vector) vector).set(index, (int) (value & 0xFFFFFFFFL));
    } else if (vector instanceof UInt8Vector) {
      ((UInt8Vector) vector).set(index, value);
    } else if (vector instanceof Float4Vector) {
      float floatValue = Float.intBitsToFloat((int) value);
      ((Float4Vector) vector).set(index, floatValue);
    } else if (vector instanceof Float8Vector) {
      double doubleValue = Double.longBitsToDouble(value);
      ((Float8Vector) vector).set(index, doubleValue);
    } else {
      throw new UnsupportedOperationException(
          "RleDecoder does not yet support vector type: " + vector.getClass().getName());
    }
  }
}
