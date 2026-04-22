// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.buffer.PageBufferStore;
import com.github.jlance.format.decoder.FullZipLayoutDecoder;
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
 * Unit tests for V2.1+ Dictionary encoding via FullZipLayout.
 */
public class V21DictionaryTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  /** Builds a FullZipLayout PageLayout with the given value compression. */
  private static EncodingsV21.PageLayout buildFullZipLayout(
      EncodingsV21.CompressiveEncoding valueCompression, int numRows, int numVisibleItems) {
    var fullZip = EncodingsV21.FullZipLayout.newBuilder()
        .setBitsRep(0)
        .setBitsDef(0)
        .setBitsPerValue(32)
        .setNumItems(numRows)
        .setNumVisibleItems(numVisibleItems)
        .setValueCompression(valueCompression)
        .build();
    return EncodingsV21.PageLayout.newBuilder().setFullZipLayout(fullZip).build();
  }

  /** Builds a FullZipLayout with nullable item layer and def buffer. */
  private static EncodingsV21.PageLayout buildFullZipLayoutNullable(
      EncodingsV21.CompressiveEncoding valueCompression, int numRows, int numVisibleItems) {
    var fullZip = EncodingsV21.FullZipLayout.newBuilder()
        .setBitsRep(0)
        .setBitsDef(1)
        .setBitsPerValue(32)
        .setNumItems(numRows)
        .setNumVisibleItems(numVisibleItems)
        .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
        .setValueCompression(valueCompression)
        .build();
    return EncodingsV21.PageLayout.newBuilder().setFullZipLayout(fullZip).build();
  }

  @Test
  public void testDictionaryInt32() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 6;
      // Dictionary items: [100, 200, 300, 400]
      int[] items = {100, 200, 300, 400};
      byte[] itemsBuf = new byte[items.length * 4];
      ByteBuffer.wrap(itemsBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(items[0]).putInt(items[1]).putInt(items[2]).putInt(items[3]);

      // Indices: [0, 1, 0, 2, 3, 1] → values [100, 200, 100, 300, 400, 200]
      int[] indices = {0, 1, 0, 2, 3, 1};
      byte[] indicesBuf = new byte[indices.length * 4];
      ByteBuffer.wrap(indicesBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(indices[0]).putInt(indices[1]).putInt(indices[2])
          .putInt(indices[3]).putInt(indices[4]).putInt(indices[5]);

      var itemsFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var itemsEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(itemsFlat).build();
      var indicesFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var indicesEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(indicesFlat).build();
      var dictionary = EncodingsV21.Dictionary.newBuilder()
          .setItems(itemsEncoding)
          .setIndices(indicesEncoding)
          .setNumDictionaryItems(items.length)
          .build();
      var dictEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setDictionary(dictionary).build();
      var pageLayout = buildFullZipLayout(dictEncoding, numRows, numRows);

      PageBufferStore store = new PageBufferStore(List.of(itemsBuf, indicesBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(100, intVec.get(0));
      assertEquals(200, intVec.get(1));
      assertEquals(100, intVec.get(2));
      assertEquals(300, intVec.get(3));
      assertEquals(400, intVec.get(4));
      assertEquals(200, intVec.get(5));
      vector.close();
    }
  }

  @Test
  public void testDictionaryWith8BitIndices() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      // Dictionary with 3 items
      int[] items = {1000, 2000, 3000};
      byte[] itemsBuf = new byte[items.length * 4];
      ByteBuffer.wrap(itemsBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(items[0]).putInt(items[1]).putInt(items[2]);

      // 8-bit indices: [0, 2, 1, 0, 2]
      byte[] indicesBuf = new byte[] {0, 2, 1, 0, 2};

      var itemsFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var itemsEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(itemsFlat).build();
      var indicesFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(8).build();
      var indicesEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(indicesFlat).build();
      var dictionary = EncodingsV21.Dictionary.newBuilder()
          .setItems(itemsEncoding)
          .setIndices(indicesEncoding)
          .setNumDictionaryItems(items.length)
          .build();
      var dictEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setDictionary(dictionary).build();
      var pageLayout = buildFullZipLayout(dictEncoding, numRows, numRows);

      PageBufferStore store = new PageBufferStore(List.of(itemsBuf, indicesBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(1000, intVec.get(0));
      assertEquals(3000, intVec.get(1));
      assertEquals(2000, intVec.get(2));
      assertEquals(1000, intVec.get(3));
      assertEquals(3000, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testDictionaryWithNullable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 5;
      int numVisible = 3;
      // Dictionary items: [10, 20]
      int[] items = {10, 20};
      byte[] itemsBuf = new byte[items.length * 4];
      ByteBuffer.wrap(itemsBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(items[0]).putInt(items[1]);

      // Indices for visible items only: [0, 1, 0]
      byte[] indicesBuf = new byte[] {0, 1, 0};

      // Def bitmap: rows 0,2,4 valid → 0b00010101 = 0x15
      byte[] defBitmap = new byte[] {0x15};

      var itemsFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var itemsEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(itemsFlat).build();
      var indicesFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(8).build();
      var indicesEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(indicesFlat).build();
      var dictionary = EncodingsV21.Dictionary.newBuilder()
          .setItems(itemsEncoding)
          .setIndices(indicesEncoding)
          .setNumDictionaryItems(items.length)
          .build();
      var dictEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setDictionary(dictionary).build();
      var pageLayout = buildFullZipLayoutNullable(dictEncoding, numRows, numVisible);

      // Buffer order: def buffer, then items, then indices
      PageBufferStore store = new PageBufferStore(List.of(defBitmap, itemsBuf, indicesBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(10, intVec.get(0));
      assertTrue(intVec.isNull(1));
      assertEquals(20, intVec.get(2));
      assertTrue(intVec.isNull(3));
      assertEquals(10, intVec.get(4));
      vector.close();
    }
  }

  @Test
  public void testDictionaryWrappedInGeneralZstd() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 4;
      int[] items = {100, 200, 300};
      byte[] itemsBuf = new byte[items.length * 4];
      ByteBuffer.wrap(itemsBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(items[0]).putInt(items[1]).putInt(items[2]);

      int[] indices = {2, 0, 1, 2};
      byte[] indicesBuf = new byte[indices.length * 4];
      ByteBuffer.wrap(indicesBuf).order(ByteOrder.LITTLE_ENDIAN)
          .putInt(indices[0]).putInt(indices[1]).putInt(indices[2]).putInt(indices[3]);

      // Wrap items in General(zstd)
      byte[] itemsCompressed = compressZstd(itemsBuf);
      var itemsInnerFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var itemsInner = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(itemsInnerFlat).build();
      var itemsGeneral = EncodingsV21.General.newBuilder()
          .setCompression(EncodingsV21.BufferCompression.newBuilder()
              .setScheme(EncodingsV21.CompressionScheme.COMPRESSION_ALGORITHM_ZSTD)
              .build())
          .setValues(itemsInner)
          .build();
      var itemsEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setGeneral(itemsGeneral).build();

      // Indices are plain Flat
      var indicesFlat = EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build();
      var indicesEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setFlat(indicesFlat).build();

      var dictionary = EncodingsV21.Dictionary.newBuilder()
          .setItems(itemsEncoding)
          .setIndices(indicesEncoding)
          .setNumDictionaryItems(items.length)
          .build();
      var dictEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setDictionary(dictionary).build();
      var pageLayout = buildFullZipLayout(dictEncoding, numRows, numRows);

      PageBufferStore store = new PageBufferStore(List.of(itemsCompressed, indicesBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(300, intVec.get(0));
      assertEquals(100, intVec.get(1));
      assertEquals(200, intVec.get(2));
      assertEquals(300, intVec.get(3));
      vector.close();
    }
  }

  private static byte[] compressZstd(byte[] data) {
    byte[] frame = com.github.luben.zstd.Zstd.compress(data, 3);
    byte[] result = new byte[8 + frame.length];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(data.length);
    System.arraycopy(frame, 0, result, 8, frame.length);
    return result;
  }
}
