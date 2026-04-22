// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Lance V2.1 files in milestone_v21/.
 */
public class MilestoneV21CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_v21", name);
  }

  @Test
  public void testSmall() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_small.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("x");
        assertEquals(0, vec.get(0));
        assertEquals(1, vec.get(1));
        assertEquals(2, vec.get(2));
        assertEquals(3, vec.get(3));
      }
    }
  }

  @Test
  public void testNullable() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_nullable.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("int32_nullable");
        assertEquals(10, vec.get(0));
        assertTrue(vec.isNull(1));
        assertEquals(0, vec.get(2));
        assertTrue(vec.isNull(3));
        assertEquals(30, vec.get(4));
      }
    }
  }

  @Test
  public void testPrimitives() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_primitives.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        IntVector intVec = (IntVector) root.getVector("int32_col");
        Float8Vector floatVec = (Float8Vector) root.getVector("float64_col");
        assertEquals(1, intVec.get(0));
        assertEquals(0, intVec.get(1));
        assertEquals(2, intVec.get(2));
        assertEquals(0, intVec.get(3));
        assertEquals(3, intVec.get(4));
        assertEquals(1.1, floatVec.get(0), 1e-9);
        assertEquals(2.2, floatVec.get(1), 1e-9);
        assertEquals(3.3, floatVec.get(2), 1e-9);
        assertEquals(4.4, floatVec.get(3), 1e-9);
        assertEquals(5.5, floatVec.get(4), 1e-9);
      }
    }
  }

  @Test
  public void testBitpack() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_bitpack.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(8000, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("x");
        // Just verify we can read all values without error
        assertEquals(8000, vec.getValueCount());
      }
    }
  }

  @Test
  public void testFullzip() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_fullzip.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10000, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("big_int");
        assertEquals(10000, vec.getValueCount());
      }
    }
  }

  @Test
  public void testHighEntropy() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_high_entropy.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(8000, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("x");
        assertEquals(8000, vec.getValueCount());
      }
    }
  }

  @Test
  public void testNoRle() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_no_rle.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(10240, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("x");
        assertEquals(10240, vec.getValueCount());
      }
    }
  }

  @Test
  public void testRle() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_rle.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(9000, root.getRowCount());
        IntVector vec = (IntVector) root.getVector("x");
        assertEquals(9000, vec.getValueCount());
      }
    }
  }
}
