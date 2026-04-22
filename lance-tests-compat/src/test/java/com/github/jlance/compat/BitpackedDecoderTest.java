// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.BitpackedDecoder;
import java.util.ArrayList;
import java.util.List;
import lance.encodings.EncodingsV20;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class BitpackedDecoderTest {

  /** Packs integer values into a byte array using LSB-first bit order. */
  private static byte[] packBits(int[] values, int bitsPerValue) {
    int totalBits = values.length * bitsPerValue;
    int totalBytes = (totalBits + 7) / 8;
    byte[] data = new byte[totalBytes];
    for (int i = 0; i < values.length; i++) {
      long value = values[i] & ((1L << bitsPerValue) - 1);
      for (int b = 0; b < bitsPerValue; b++) {
        if ((value & (1L << b)) != 0) {
          long bitPos = (long) i * bitsPerValue + b;
          int byteIdx = (int) (bitPos / 8);
          int bitIdx = (int) (bitPos % 8);
          data[byteIdx] |= (1 << bitIdx);
        }
      }
    }
    return data;
  }

  private static EncodingsV20.ArrayEncoding makeBitpackedEncoding(
      int compressedBits, int uncompressedBits, boolean signed) {
    var bitpacked =
        EncodingsV20.Bitpacked.newBuilder()
            .setCompressedBitsPerValue(compressedBits)
            .setUncompressedBitsPerValue(uncompressedBits)
            .setSigned(signed)
            .setBuffer(
                EncodingsV20.Buffer.newBuilder()
                    .setBufferIndex(0)
                    .setBufferType(EncodingsV20.Buffer.BufferType.page))
            .build();
    return EncodingsV20.ArrayEncoding.newBuilder().setBitpacked(bitpacked).build();
  }

  @Test
  public void testUnsigned3BitIntegers() throws Exception {
    int[] values = {0, 1, 2, 3, 4, 5, 6, 7};
    byte[] data = packBits(values, 3);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(3, 32, false);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        assertEquals(values.length, intVec.getValueCount());
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testSigned3BitIntegers() throws Exception {
    int[] values = {-4, -3, -2, -1, 0, 1, 2, 3};
    int[] packed = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      packed[i] = values[i] < 0 ? values[i] + 8 : values[i];
    }
    byte[] data = packBits(packed, 3);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(3, 32, true);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testBoolean1Bit() throws Exception {
    int[] values = {1, 0, 1, 1, 0, 0, 1, 0, 1, 1};
    byte[] data = packBits(values, 1);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(1, 1, false);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(ArrowType.Bool.INSTANCE), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        BitVector bitVec = (BitVector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], bitVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testCrossByteBoundary() throws Exception {
    int[] values = {0, 15, 31, 7, 20};
    byte[] data = packBits(values, 5);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(5, 32, false);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(32, true)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        IntVector intVec = (IntVector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], intVec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void test16BitUncompressed() throws Exception {
    int[] values = {0, 5, 10, 15};
    byte[] data = packBits(values, 4);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(4, 16, false);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(16, true)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        SmallIntVector vec = (SmallIntVector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void test64BitUnsigned() throws Exception {
    int[] values = {0, 21, 42, 63};
    byte[] data = packBits(values, 6);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(6, 64, false);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(64, false)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        UInt8Vector vec = (UInt8Vector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }

  @Test
  public void testSigned8BitToTinyInt() throws Exception {
    int[] values = {-8, -4, 0, 3, 7};
    int[] packed = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      packed[i] = values[i] < 0 ? values[i] + 16 : values[i];
    }
    byte[] data = packBits(packed, 4);
    List<byte[]> buffers = new ArrayList<>();
    buffers.add(data);
    PageBufferStore store = new PageBufferStore(buffers);
    EncodingsV20.ArrayEncoding encoding = makeBitpackedEncoding(4, 8, true);

    try (BufferAllocator allocator = new RootAllocator()) {
      Field field = new Field("a", FieldType.nullable(new ArrowType.Int(8, true)), null);
      BitpackedDecoder decoder = new BitpackedDecoder();
      FieldVector vector = decoder.decode(encoding, values.length, store, field, allocator);
      try {
        TinyIntVector vec = (TinyIntVector) vector;
        for (int i = 0; i < values.length; i++) {
          assertEquals(values[i], vec.get(i), "Mismatch at index " + i);
        }
      } finally {
        vector.close();
      }
    }
  }
}
