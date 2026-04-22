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
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

public class Milestone09CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_09", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testLargeTypesSinglePage() throws Exception {
    Path file = findLanceFile("test_large_types");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        LargeVarCharVector largeStr = (LargeVarCharVector) root.getVector("large_str");
        assertEquals(new Text("hello"), largeStr.getObject(0));
        assertEquals(new Text("world"), largeStr.getObject(1));
        assertTrue(largeStr.isNull(2));
        assertEquals(new Text("large string test"), largeStr.getObject(3));
        assertEquals(new Text(""), largeStr.getObject(4));

        LargeVarBinaryVector largeBin = (LargeVarBinaryVector) root.getVector("large_bin");
        assertArrayEquals(new byte[]{0x00, 0x01, 0x02}, largeBin.getObject(0));
        assertArrayEquals(new byte[]{(byte) 0xff, (byte) 0xfe}, largeBin.getObject(1));
        assertTrue(largeBin.isNull(2));
        assertArrayEquals("binary data here".getBytes(), largeBin.getObject(3));
        assertArrayEquals(new byte[]{}, largeBin.getObject(4));

        LargeListVector largeList = (LargeListVector) root.getVector("large_list");
        assertFalse(largeList.isNull(0));
        assertEquals(java.util.List.of(1, 2, 3), largeList.getObject(0));
        assertFalse(largeList.isNull(1));
        assertEquals(java.util.List.of(), largeList.getObject(1));
        assertTrue(largeList.isNull(2));
        assertFalse(largeList.isNull(3));
        assertEquals(java.util.List.of(4, 5), largeList.getObject(3));
        assertFalse(largeList.isNull(4));
        assertEquals(java.util.List.of(6, 7, 8, 9, 10), largeList.getObject(4));
      }
    }
  }

  @Test
  public void testLargeStringMultiPage() throws Exception {
    Path file = findLanceFile("test_large_string_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100_000, root.getRowCount());

        LargeVarCharVector largeStr = (LargeVarCharVector) root.getVector("large_str_multi");
        assertFalse(largeStr.isNull(0));
        assertFalse(largeStr.isNull(99999));
        assertEquals(new Text("row_0_"), largeStr.getObject(0));
      }
    }
  }
}
