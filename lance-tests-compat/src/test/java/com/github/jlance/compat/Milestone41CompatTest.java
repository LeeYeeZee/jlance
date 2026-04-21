package com.github.jlance.compat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for V2.0 PackedStruct encoding.
 */
public class Milestone41CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_41");

  @Test
  public void testPackedStructBasic() throws Exception {
    Path file = DATA_DIR.resolve("test_packed_struct_basic.lance");
    if (!java.nio.file.Files.exists(file)) {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "Test data not generated: " + file);
    }

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = new LanceFileReader(file)) {
      var metadata = reader.readMetadata();
      assertEquals(50, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("packed_struct");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(50, struct.getValueCount());

        FieldVector xVec = struct.getChild("x");
        FieldVector yVec = struct.getChild("y");
        assertInstanceOf(IntVector.class, xVec);
        assertInstanceOf(Float8Vector.class, yVec);

        IntVector x = (IntVector) xVec;
        Float8Vector y = (Float8Vector) yVec;

        for (int i = 0; i < 50; i++) {
          assertEquals(i, x.get(i), "x[" + i + "]");
          assertEquals(i * 1.5, y.get(i), 1e-9, "y[" + i + "]");
        }
      }
    }
  }

  @Test
  public void testPackedStructMultipleTypes() throws Exception {
    Path file = DATA_DIR.resolve("test_packed_struct_types.lance");
    if (!java.nio.file.Files.exists(file)) {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "Test data not generated: " + file);
    }

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = new LanceFileReader(file)) {
      var metadata = reader.readMetadata();
      assertEquals(30, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("packed");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(30, struct.getValueCount());

        org.apache.arrow.vector.TinyIntVector a =
            (org.apache.arrow.vector.TinyIntVector) struct.getChild("a");
        org.apache.arrow.vector.SmallIntVector b =
            (org.apache.arrow.vector.SmallIntVector) struct.getChild("b");
        IntVector c = (IntVector) struct.getChild("c");
        org.apache.arrow.vector.BigIntVector d =
            (org.apache.arrow.vector.BigIntVector) struct.getChild("d");
        org.apache.arrow.vector.Float4Vector e =
            (org.apache.arrow.vector.Float4Vector) struct.getChild("e");
        Float8Vector f = (Float8Vector) struct.getChild("f");

        for (int i = 0; i < 30; i++) {
          assertEquals((byte) ((i % 128) - 64), a.get(i), "a[" + i + "]");
          assertEquals((short) ((i % 32768) - 16384), b.get(i), "b[" + i + "]");
          assertEquals(i * 100, c.get(i), "c[" + i + "]");
          assertEquals(i * 1000000L, d.get(i), "d[" + i + "]");
          assertEquals((float) (i * 0.5), e.get(i), 1e-6f, "e[" + i + "]");
          assertEquals(i * 0.25, f.get(i), 1e-9, "f[" + i + "]");
        }
      }
    }
  }

  @Test
  public void testPackedStructFixedSizeList() throws Exception {
    Path file = DATA_DIR.resolve("test_packed_struct_fsl.lance");
    if (!java.nio.file.Files.exists(file)) {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "Test data not generated: " + file);
    }

    try (BufferAllocator allocator = new RootAllocator();
         LanceFileReader reader = new LanceFileReader(file)) {
      var metadata = reader.readMetadata();
      assertEquals(20, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("packed");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(20, struct.getValueCount());

        IntVector id = (IntVector) struct.getChild("id");
        FixedSizeListVector coords =
            (FixedSizeListVector) struct.getChild("coords");

        for (int i = 0; i < 20; i++) {
          assertEquals(i, id.get(i), "id[" + i + "]");
          org.apache.arrow.vector.Float4Vector inner =
              (org.apache.arrow.vector.Float4Vector) coords.getDataVector();
          int base = i * 3;
          assertEquals((float) i, inner.get(base), 1e-6f, "coords[" + i + "][0]");
          assertEquals((float) (i + 1), inner.get(base + 1), 1e-6f, "coords[" + i + "][1]");
          assertEquals((float) (i + 2), inner.get(base + 2), 1e-6f, "coords[" + i + "][2]");
        }
      }
    }
  }
}
