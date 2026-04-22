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
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone11CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_11", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testHalffloatSinglePage() throws Exception {
    Path file = findLanceFile("test_halffloat");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        Float2Vector hf = (Float2Vector) root.getVector("halffloat_col");
        assertEquals(0.0f, hf.getValueAsFloat(0), 1e-3f);
        assertEquals(1.5f, hf.getValueAsFloat(1), 1e-3f);
        assertEquals(-2.5f, hf.getValueAsFloat(2), 1e-3f);
        assertTrue(hf.isNull(3));
        assertEquals(3.25f, hf.getValueAsFloat(4), 1e-3f);
      }
    }
  }

  @Test
  public void testHalffloatMultiPage() throws Exception {
    Path file = findLanceFile("test_halffloat_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100_000, root.getRowCount());

        Float2Vector hf = (Float2Vector) root.getVector("halffloat_multi");
        assertFalse(hf.isNull(0));
        assertFalse(hf.isNull(99999));
      }
    }
  }

  @Test
  public void testFixedSizeBinarySinglePage() throws Exception {
    Path file = findLanceFile("test_fixed_size_binary");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        FixedSizeBinaryVector fsb = (FixedSizeBinaryVector) root.getVector("fsb_col");
        assertArrayEquals(new byte[] {'a', 'b', 'c', 'd'}, fsb.get(0));
        assertArrayEquals(new byte[] {'e', 'f', 'g', 'h'}, fsb.get(1));
        assertArrayEquals(new byte[] {'i', 'j', 'k', 'l'}, fsb.get(2));
        assertTrue(fsb.isNull(3));
        assertArrayEquals(new byte[] {'m', 'n', 'o', 'p'}, fsb.get(4));
      }
    }
  }
}
