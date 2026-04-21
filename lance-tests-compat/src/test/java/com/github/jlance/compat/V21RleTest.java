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
 * Unit tests for V2.1+ RLE encoding via FullZipLayout (Block path).
 */
public class V21RleTest {

  private static Field int32Field(String name) {
    return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
  }

  /** Builds RLE buffer: [8-byte LE values size][values][lengths]. */
  private static byte[] buildRleBuffer(int[] values, int[] lengths) {
    int valuesBytes = values.length * 4;
    byte[] result = new byte[8 + valuesBytes + lengths.length];
    ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN).putLong(valuesBytes);
    ByteBuffer valBuf = ByteBuffer.wrap(result, 8, valuesBytes).order(ByteOrder.LITTLE_ENDIAN);
    for (int v : values) {
      valBuf.putInt(v);
    }
    for (int i = 0; i < lengths.length; i++) {
      result[8 + valuesBytes + i] = (byte) lengths[i];
    }
    return result;
  }

  @Test
  public void testRleBasic() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 9;
      // Values: [1, 2, 3]  | Lengths: [3, 2, 4]  → [1,1,1,2,2,3,3,3,3]
      int[] values = {1, 2, 3};
      int[] lengths = {3, 2, 4};
      byte[] rleBuf = buildRleBuffer(values, lengths);

      var rle = EncodingsV21.Rle.newBuilder()
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build())
              .build())
          .setRunLengths(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(8).build())
              .build())
          .build();
      var rleEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setRle(rle).build();

      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .setValueCompression(rleEncoding)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(rleBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(1, intVec.get(0));
      assertEquals(1, intVec.get(1));
      assertEquals(1, intVec.get(2));
      assertEquals(2, intVec.get(3));
      assertEquals(2, intVec.get(4));
      assertEquals(3, intVec.get(5));
      assertEquals(3, intVec.get(6));
      assertEquals(3, intVec.get(7));
      assertEquals(3, intVec.get(8));
      vector.close();
    }
  }

  @Test
  public void testRleLongRunSplit() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 300;
      // Single value 42 repeated 300 times → split into 255 + 45
      int[] values = {42, 42};
      int[] lengths = {255, 45};
      byte[] rleBuf = buildRleBuffer(values, lengths);

      var rle = EncodingsV21.Rle.newBuilder()
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build())
              .build())
          .setRunLengths(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(8).build())
              .build())
          .build();
      var rleEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setRle(rle).build();

      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(0)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numRows)
          .setValueCompression(rleEncoding)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      PageBufferStore store = new PageBufferStore(List.of(rleBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      for (int i = 0; i < numRows; i++) {
        assertEquals(42, intVec.get(i), "Mismatch at index " + i);
      }
      vector.close();
    }
  }

  @Test
  public void testRleWithNullable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator()) {
      int numRows = 6;
      int numVisible = 4;
      // Valid rows: 0,1,3,5 (4 visible)
      // RLE for visible values: [10, 20] with lengths [2, 2]
      int[] values = {10, 20};
      int[] lengths = {2, 2};
      byte[] rleBuf = buildRleBuffer(values, lengths);
      // Def bitmap: rows 0,1 valid; row 2 null; rows 3,4,5 valid? Wait, only 4 visible.
      // Validity: 0=T,1=T,2=F,3=T,4=F,5=T → bitmap = 0b00101011 = 0x2B
      byte[] defBitmap = new byte[] {0x2B};

      var rle = EncodingsV21.Rle.newBuilder()
          .setValues(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(32).build())
              .build())
          .setRunLengths(EncodingsV21.CompressiveEncoding.newBuilder()
              .setFlat(EncodingsV21.Flat.newBuilder().setBitsPerValue(8).build())
              .build())
          .build();
      var rleEncoding = EncodingsV21.CompressiveEncoding.newBuilder().setRle(rle).build();

      var fullZip = EncodingsV21.FullZipLayout.newBuilder()
          .setBitsRep(0)
          .setBitsDef(1)
          .setBitsPerValue(32)
          .setNumItems(numRows)
          .setNumVisibleItems(numVisible)
          .addLayers(EncodingsV21.RepDefLayer.REPDEF_NULLABLE_ITEM)
          .setValueCompression(rleEncoding)
          .build();
      var pageLayout = EncodingsV21.PageLayout.newBuilder()
          .setFullZipLayout(fullZip)
          .build();

      // Buffer order: def, then RLE value buffer
      PageBufferStore store = new PageBufferStore(List.of(defBitmap, rleBuf));
      FullZipLayoutDecoder decoder = new FullZipLayoutDecoder();
      FieldVector vector = decoder.decode(pageLayout, numRows, store, int32Field("x"), allocator);

      assertInstanceOf(IntVector.class, vector);
      IntVector intVec = (IntVector) vector;
      assertEquals(numRows, intVec.getValueCount());
      assertEquals(10, intVec.get(0));
      assertEquals(10, intVec.get(1));
      assertTrue(intVec.isNull(2));
      assertEquals(20, intVec.get(3));
      assertTrue(intVec.isNull(4));
      assertEquals(20, intVec.get(5));
      vector.close();
    }
  }
}
