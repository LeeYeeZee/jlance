// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

public class Milestone03CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_03", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testInt32() throws Exception {
    Path file = findLanceFile("test_int32");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        assertEquals(1, root.getFieldVectors().size());

        IntVector vec = (IntVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(5, vec.getValueCount());
        assertEquals(1, vec.get(0));
        assertEquals(2, vec.get(1));
        assertEquals(3, vec.get(2));
        assertEquals(4, vec.get(3));
        assertEquals(5, vec.get(4));
        assertEquals(0, vec.getNullCount());
      }
    }
  }

  @Test
  public void testFloat64Nullable() throws Exception {
    Path file = findLanceFile("test_float64_nullable");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        assertEquals(1, root.getFieldVectors().size());

        Float8Vector vec = (Float8Vector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(5, vec.getValueCount());
        assertEquals(1.1, vec.get(0), 1e-9);
        assertTrue(vec.isNull(1));
        assertEquals(3.3, vec.get(2), 1e-9);
        assertTrue(vec.isNull(3));
        assertEquals(5.5, vec.get(4), 1e-9);
        assertEquals(2, vec.getNullCount());
      }
    }
  }

  @Test
  public void testStruct() throws Exception {
    Path file = findLanceFile("test_struct");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(3, root.getRowCount());
        assertEquals(1, root.getFieldVectors().size());

        StructVector vec = (StructVector) root.getVector("s");
        assertNotNull(vec);
        assertEquals(3, vec.getValueCount());

        IntVector xVec = vec.getChild("x", IntVector.class);
        assertNotNull(xVec);
        assertEquals(1, xVec.get(0));
        assertEquals(2, xVec.get(1));
        assertEquals(3, xVec.get(2));
        assertEquals(0, xVec.getNullCount());

        org.apache.arrow.vector.Float4Vector yVec =
            vec.getChild("y", org.apache.arrow.vector.Float4Vector.class);
        assertNotNull(yVec);
        assertEquals(1.1f, yVec.get(0), 1e-6f);
        assertEquals(2.2f, yVec.get(1), 1e-6f);
        assertTrue(yVec.isNull(2));
        assertEquals(1, yVec.getNullCount());
      }
    }
  }
}
