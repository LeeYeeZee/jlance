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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone14CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_14", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testAllNullInt32() throws Exception {
    Path file = findLanceFile("test_all_null_int32");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        IntVector vec = (IntVector) root.getVector("all_null_int");
        assertTrue(vec.isNull(0));
        assertTrue(vec.isNull(1));
        assertTrue(vec.isNull(2));
        assertTrue(vec.isNull(3));
        assertTrue(vec.isNull(4));
      }
    }
  }

  @Test
  public void testConstantInt32() throws Exception {
    Path file = findLanceFile("test_constant_int32");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        IntVector vec = (IntVector) root.getVector("same_int");
        assertEquals(42, vec.get(0));
        assertEquals(42, vec.get(1));
        assertEquals(42, vec.get(2));
        assertEquals(42, vec.get(3));
        assertEquals(42, vec.get(4));
      }
    }
  }
}
