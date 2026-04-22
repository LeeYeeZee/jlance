// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.buffer.PageBufferStore;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a {@link lance.encodings.EncodingsV20.Bitpacked} encoding into an Arrow vector.
 *
 * <p>Values are packed with a fixed number of bits per value, starting from the least
 * significant bit (LSB-first / little-endian bit order).
 */
public class BitpackedDecoder implements ArrayDecoder {

  @Override
  public FieldVector decode(
      ArrayEncoding encoding,
      int numRows,
      PageBufferStore store,
      Field field,
      BufferAllocator allocator) {
    var bitpacked = encoding.getBitpacked();
    int compressedBits = (int) bitpacked.getCompressedBitsPerValue();
    int uncompressedBits = (int) bitpacked.getUncompressedBitsPerValue();
    boolean signed = bitpacked.getSigned();
    int bufferIndex = bitpacked.getBuffer().getBufferIndex();
    byte[] data = store.getBuffer(bufferIndex);

    if (compressedBits <= 0 || uncompressedBits <= 0) {
      throw new IllegalArgumentException(
          "Invalid bitpacked bits: compressed=" + compressedBits
              + ", uncompressed=" + uncompressedBits);
    }

    FieldVector vector = field.createVector(allocator);
    vector.setInitialCapacity(numRows);
    vector.allocateNew();

    for (int i = 0; i < numRows; i++) {
      long rawValue = readBits(data, (long) i * compressedBits, compressedBits);

      if (signed && compressedBits < 64) {
        rawValue = signExtend(rawValue, compressedBits);
      }

      // Write value into the appropriate vector type.
      if (vector instanceof BitVector) {
        ((BitVector) vector).set(i, rawValue != 0 ? 1 : 0);
      } else if (vector instanceof TinyIntVector) {
        ((TinyIntVector) vector).set(i, (byte) rawValue);
      } else if (vector instanceof SmallIntVector) {
        ((SmallIntVector) vector).set(i, (short) rawValue);
      } else if (vector instanceof IntVector) {
        ((IntVector) vector).set(i, (int) rawValue);
      } else if (vector instanceof BigIntVector) {
        ((BigIntVector) vector).set(i, rawValue);
      } else if (vector instanceof UInt1Vector) {
        ((UInt1Vector) vector).set(i, (byte) (rawValue & 0xFF));
      } else if (vector instanceof UInt2Vector) {
        ((UInt2Vector) vector).set(i, (char) (rawValue & 0xFFFF));
      } else if (vector instanceof UInt4Vector) {
        ((UInt4Vector) vector).set(i, (int) (rawValue & 0xFFFFFFFFL));
      } else if (vector instanceof UInt8Vector) {
        ((UInt8Vector) vector).set(i, rawValue);
      } else {
        throw new UnsupportedOperationException(
            "BitpackedDecoder does not yet support vector type: "
                + vector.getClass().getName());
      }
    }

    vector.setValueCount(numRows);
    return vector;
  }

  /**
   * Reads {@code numBits} bits from {@code data} starting at {@code bitOffset}.
   *
   * <p>Bits are read in LSB-first order (little-endian bit order), matching Lance's
   * convention.
   */
  private static long readBits(byte[] data, long bitOffset, int numBits) {
    long result = 0;
    for (int i = 0; i < numBits; i++) {
      long pos = bitOffset + i;
      int byteIdx = (int) (pos / 8);
      int bitIdx = (int) (pos % 8);
      if ((data[byteIdx] & (1 << bitIdx)) != 0) {
        result |= (1L << i);
      }
    }
    return result;
  }

  /** Sign-extends a value from {@code bits} width to 64 bits. */
  private static long signExtend(long value, int bits) {
    long signBit = 1L << (bits - 1);
    if ((value & signBit) != 0) {
      // Negative: extend 1s into high bits.
      return value | (~0L << bits);
    }
    // Positive: high bits already zero.
    return value;
  }
}
