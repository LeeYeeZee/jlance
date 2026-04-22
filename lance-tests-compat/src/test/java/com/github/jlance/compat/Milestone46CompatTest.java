// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

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
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for V2.1 Struct support.
 */
public class Milestone46CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_46");

  private static Path dataPath(String name) {
    return DATA_DIR.resolve(name);
  }

  @Test
  public void testStructV21Basic() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_struct_v21_basic.lance"))) {
      var metadata = reader.readMetadata();
      assertEquals(20, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("s");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(20, struct.getValueCount());

        IntVector x = (IntVector) struct.getChild("x");
        Float8Vector y = (Float8Vector) struct.getChild("y");

        for (int i = 0; i < 20; i++) {
          assertEquals(i, x.get(i), "x[" + i + "]");
          assertEquals(i * 1.5, y.get(i), 1e-9, "y[" + i + "]");
        }
      }
    }
  }

  @Test
  public void testStructV21Nullable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_struct_v21_nullable.lance"))) {
      var metadata = reader.readMetadata();
      assertEquals(5, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("s");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(5, struct.getValueCount());

        IntVector x = (IntVector) struct.getChild("x");
        Float8Vector y = (Float8Vector) struct.getChild("y");

        assertFalse(struct.isNull(0));
        assertEquals(1, x.get(0));
        assertEquals(1.5, y.get(0), 1e-9);

        assertTrue(struct.isNull(1));
        assertTrue(x.isNull(1));
        assertTrue(y.isNull(1));

        assertFalse(struct.isNull(2));
        assertEquals(3, x.get(2));
        assertEquals(3.5, y.get(2), 1e-9);

        assertTrue(struct.isNull(3));
        assertTrue(x.isNull(3));
        assertTrue(y.isNull(3));

        assertFalse(struct.isNull(4));
        assertEquals(5, x.get(4));
        assertEquals(5.5, y.get(4), 1e-9);
      }
    }
  }

  @Test
  public void testStructV21NullableChildren() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_struct_v21_nullable_children.lance"))) {
      var metadata = reader.readMetadata();
      assertEquals(5, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("s");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(5, struct.getValueCount());

        IntVector x = (IntVector) struct.getChild("x");
        Float8Vector y = (Float8Vector) struct.getChild("y");

        // Row 0: both valid
        assertFalse(struct.isNull(0));
        assertFalse(x.isNull(0));
        assertFalse(y.isNull(0));
        assertEquals(1, x.get(0));
        assertEquals(1.5, y.get(0), 1e-9);

        // Row 1: x null, y valid
        assertFalse(struct.isNull(1));
        assertTrue(x.isNull(1));
        assertFalse(y.isNull(1));
        assertEquals(2.5, y.get(1), 1e-9);

        // Row 2: x valid, y null
        assertFalse(struct.isNull(2));
        assertFalse(x.isNull(2));
        assertTrue(y.isNull(2));
        assertEquals(3, x.get(2));

        // Row 3: both null
        assertFalse(struct.isNull(3));
        assertTrue(x.isNull(3));
        assertTrue(y.isNull(3));

        // Row 4: both valid
        assertFalse(struct.isNull(4));
        assertFalse(x.isNull(4));
        assertFalse(y.isNull(4));
        assertEquals(5, x.get(4));
        assertEquals(5.5, y.get(4), 1e-9);
      }
    }
  }
}
