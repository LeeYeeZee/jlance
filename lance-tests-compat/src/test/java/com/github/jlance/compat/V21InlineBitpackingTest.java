// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.InlineBitpackingDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import lance.encodings21.EncodingsV21;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for V2.1+ InlineBitpacking (FastLanes) decoding.
 */
public class V21InlineBitpackingTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  /**
   * Builds a u32 InlineBitpacking buffer with bit_width=32 (raw passthrough).
   *
   * <p>Each chunk always contains packed data for 1024 values, even if the
   * logical value count is smaller (the tail is padded).
   */
  private static byte[] buildInlineU32Buffer(int[] values) {
    int numChunks = (values.length + 1023) / 1024;
    int bufSize = numChunks * (4 + 1024 * 4); // 4 bytes metadata + 1024 ints per chunk
    byte[] result = new byte[bufSize];
    ByteBuffer bb = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < numChunks; i++) {
      bb.putInt(32); // bit_width = 32
      for (int j = 0; j < 1024; j++) {
        int idx = i * 1024 + j;
        bb.putInt(idx < values.length ? values[idx] : 0); // pad tail with 0
      }
    }
    return result;
  }

  @Test
  public void testInlineBitpackingU32Basic() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 5;
      int[] values = {10, 20, 30, 40, 50};
      byte[] buf = buildInlineU32Buffer(values);

      var encoding = EncodingsV21.InlineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(buf));
      FieldVector vector = InlineBitpackingDecoder.decode(encoding, numValues, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      assertEquals(10, intVec.get(0));
      assertEquals(20, intVec.get(1));
      assertEquals(30, intVec.get(2));
      assertEquals(40, intVec.get(3));
      assertEquals(50, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testInlineBitpackingU32MultiChunk() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 1030; // 1 full chunk + 6 tail values
      int[] values = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        values[i] = i * 3;
      }
      byte[] buf = buildInlineU32Buffer(values);

      var encoding = EncodingsV21.InlineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(buf));
      FieldVector vector = InlineBitpackingDecoder.decode(encoding, numValues, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      for (int i = 0; i < numValues; i++) {
        assertEquals(i * 3, intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }

  @Test
  public void testInlineBitpackingU32ZeroWidth() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 5;
      // bit_width = 0 means all values are zero, no packed data follows header
      int numChunks = 1;
      byte[] buf = new byte[numChunks * 4]; // 4 bytes metadata per chunk
      ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
      bb.putInt(0); // bit_width = 0

      var encoding = EncodingsV21.InlineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(buf));
      FieldVector vector = InlineBitpackingDecoder.decode(encoding, numValues, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numValues, intVec.getValueCount());
      for (int i = 0; i < numValues; i++) {
        assertEquals(0, intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }

  @Test
  public void testInlineBitpackingViaCompressiveEncoding() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numValues = 4;
      int[] values = {7, 8, 9, 10};
      byte[] buf = buildInlineU32Buffer(values);

      var inline = EncodingsV21.InlineBitpacking.newBuilder()
          .setUncompressedBitsPerValue(32)
          .build();
      var compressive = EncodingsV21.CompressiveEncoding.newBuilder()
          .setInlineBitpacking(inline)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(buf));
      var field = int32Field("x");

      // Use CompressiveEncodingDecoders.decodeToVector
      var result = com.github.jlance.format.decoder.CompressiveEncodingDecoders.decodeToVector(
          compressive, numValues, store, field, allocator);

      assertInstanceOf(IntVector.class, result);
      IntVector intVec = (IntVector) result;
      assertEquals(numValues, intVec.getValueCount());
      assertEquals(7, intVec.get(0));
      assertEquals(8, intVec.get(1));
      assertEquals(9, intVec.get(2));
      assertEquals(10, intVec.get(3));
      result.close();
    }
  }
}
