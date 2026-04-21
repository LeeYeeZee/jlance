package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.decoder.ConstantDecoder;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import lance.encodings.EncodingsV20.ArrayEncoding;
import lance.encodings.EncodingsV20.Constant;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class ConstantDecoderCompatTest {

  @Test
  public void testConstantInt32() {
    try (BufferAllocator allocator = new RootAllocator()) {
      ArrayEncoding encoding =
          ArrayEncoding.newBuilder()
              .setConstant(
                  Constant.newBuilder()
                      .setValue(
                          com.google.protobuf.ByteString.copyFrom(
                              ByteBuffer.allocate(4)
                                  .order(ByteOrder.LITTLE_ENDIAN)
                                  .putInt(42)
                                  .array()))
                      .build())
              .build();

      Field field = new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null);
      ConstantDecoder decoder = new ConstantDecoder();
      IntVector vec = (IntVector) decoder.decode(encoding, 5, null, field, allocator);

      assertEquals(5, vec.getValueCount());
      for (int i = 0; i < 5; i++) {
        assertEquals(42, vec.get(i));
      }
      vec.close();
    }
  }

  @Test
  public void testConstantString() {
    try (BufferAllocator allocator = new RootAllocator()) {
      byte[] value = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
      ArrayEncoding encoding =
          ArrayEncoding.newBuilder()
              .setConstant(
                  Constant.newBuilder()
                      .setValue(com.google.protobuf.ByteString.copyFrom(value))
                      .build())
              .build();

      Field field = new Field("val", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
      ConstantDecoder decoder = new ConstantDecoder();
      VarCharVector vec = (VarCharVector) decoder.decode(encoding, 3, null, field, allocator);

      assertEquals(3, vec.getValueCount());
      for (int i = 0; i < 3; i++) {
        assertArrayEquals(value, vec.get(i));
      }
      vec.close();
    }
  }
}
