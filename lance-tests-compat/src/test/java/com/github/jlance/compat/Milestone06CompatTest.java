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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

public class Milestone06CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_06", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testListInt32() throws Exception {
    Path file = findLanceFile("test_list_int32");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(5, vec.getValueCount());

        // Row 0: [1, 2, 3]
        assertFalse(vec.isNull(0));
        assertEquals(0, vec.getElementStartIndex(0));
        assertEquals(3, vec.getElementEndIndex(0));
        IntVector data = (IntVector) vec.getDataVector();
        assertEquals(1, data.get(0));
        assertEquals(2, data.get(1));
        assertEquals(3, data.get(2));

        // Row 1: [4, 5]
        assertFalse(vec.isNull(1));
        assertEquals(3, vec.getElementStartIndex(1));
        assertEquals(5, vec.getElementEndIndex(1));
        assertEquals(4, data.get(3));
        assertEquals(5, data.get(4));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(5, vec.getElementStartIndex(3));
        assertEquals(5, vec.getElementEndIndex(3));

        // Row 4: [6]
        assertFalse(vec.isNull(4));
        assertEquals(5, vec.getElementStartIndex(4));
        assertEquals(6, vec.getElementEndIndex(4));
        assertEquals(6, data.get(5));
      }
    }
  }

  @Test
  public void testListString() throws Exception {
    Path file = findLanceFile("test_list_string");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(5, vec.getValueCount());

        VarCharVector data = (VarCharVector) vec.getDataVector();

        // Row 0: ["a", "bb"]
        assertFalse(vec.isNull(0));
        assertEquals(0, vec.getElementStartIndex(0));
        assertEquals(2, vec.getElementEndIndex(0));
        assertEquals("a", new String(data.get(0), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("bb", new String(data.get(1), java.nio.charset.StandardCharsets.UTF_8));

        // Row 1: ["ccc"]
        assertFalse(vec.isNull(1));
        assertEquals(2, vec.getElementStartIndex(1));
        assertEquals(3, vec.getElementEndIndex(1));
        assertEquals("ccc", new String(data.get(2), java.nio.charset.StandardCharsets.UTF_8));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(3, vec.getElementStartIndex(3));
        assertEquals(3, vec.getElementEndIndex(3));

        // Row 4: ["d", "ee", "fff"]
        assertFalse(vec.isNull(4));
        assertEquals(3, vec.getElementStartIndex(4));
        assertEquals(6, vec.getElementEndIndex(4));
        assertEquals("d", new String(data.get(3), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("ee", new String(data.get(4), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("fff", new String(data.get(5), java.nio.charset.StandardCharsets.UTF_8));
      }
    }
  }

  @Test
  public void testListInt32MultiPage() throws Exception {
    Path file = findLanceFile("test_list_int32_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100000, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(100000, vec.getValueCount());
        IntVector data = (IntVector) vec.getDataVector();

        // Spot-check a few values
        // Python generated: [[i, i+1] for i in range(100000)]
        // Row i: [i, i+1]
        assertEquals(0, vec.getElementStartIndex(0));
        assertEquals(2, vec.getElementEndIndex(0));
        assertEquals(0, data.get(0));
        assertEquals(1, data.get(1));

        assertEquals(2, vec.getElementStartIndex(1));
        assertEquals(4, vec.getElementEndIndex(1));
        assertEquals(1, data.get(2));
        assertEquals(2, data.get(3));

        assertEquals(50000 * 2, vec.getElementStartIndex(50000));
        assertEquals(50000 * 2 + 2, vec.getElementEndIndex(50000));
        assertEquals(50000, data.get(50000 * 2));
        assertEquals(50001, data.get(50000 * 2 + 1));

        assertEquals(99999 * 2, vec.getElementStartIndex(99999));
        assertEquals(99999 * 2 + 2, vec.getElementEndIndex(99999));
        assertEquals(99999, data.get(99999 * 2));
        assertEquals(100000, data.get(99999 * 2 + 1));
      }
    }
  }
}
