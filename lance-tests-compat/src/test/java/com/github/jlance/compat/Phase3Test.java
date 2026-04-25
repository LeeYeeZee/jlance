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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

public class Phase3Test {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "phase3_test");

  private static Path findFile(String name) {
    Path file = DATA_DIR.resolve(name + ".lance");
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(file), "Test data not found: " + file);
    return file;
  }

  @Test
  public void testMixedNullAndEmptyList() throws Exception {
    Path file = findFile("mixed");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("col");
        assertNotNull(vec);

        // Expected: [None, [], None, [], None]
        assertTrue(vec.isNull(0), "Row 0 should be null");
        assertFalse(vec.isNull(1), "Row 1 should not be null");
        assertEquals(0, vec.getElementEndIndex(1) - vec.getElementStartIndex(1), "Row 1 should be empty");
        assertTrue(vec.isNull(2), "Row 2 should be null");
        assertFalse(vec.isNull(3), "Row 3 should not be null");
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3), "Row 3 should be empty");
        assertTrue(vec.isNull(4), "Row 4 should be null");
      }
    }
  }

  @Test
  public void testAllEmptyList() throws Exception {
    Path file = findFile("all_empty");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("col");
        assertNotNull(vec);

        for (int i = 0; i < 20; i++) {
          assertFalse(vec.isNull(i), "Row " + i + " should not be null");
          assertEquals(0, vec.getElementEndIndex(i) - vec.getElementStartIndex(i), "Row " + i + " should be empty");
        }
      }
    }
  }

  @Test
  public void testAllNullList() throws Exception {
    Path file = findFile("all_null");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("col");
        assertNotNull(vec);

        for (int i = 0; i < 20; i++) {
          assertTrue(vec.isNull(i), "Row " + i + " should be null");
        }
      }
    }
  }
}
