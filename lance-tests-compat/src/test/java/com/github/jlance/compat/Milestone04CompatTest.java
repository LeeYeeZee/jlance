package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.junit.jupiter.api.Test;

public class Milestone04CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_04", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testString() throws Exception {
    Path file = findLanceFile("test_string");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        VarCharVector vec = (VarCharVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(4, vec.getValueCount());
        assertEquals("hello", new String(vec.get(0), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("world", new String(vec.get(1), java.nio.charset.StandardCharsets.UTF_8));
        assertTrue(vec.isNull(2));
        assertEquals("lance", new String(vec.get(3), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(1, vec.getNullCount());
      }
    }
  }

  @Test
  public void testBinary() throws Exception {
    Path file = findLanceFile("test_binary");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        VarBinaryVector vec = (VarBinaryVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(4, vec.getValueCount());
        assertArrayEquals(new byte[]{0x00, 0x01}, vec.get(0));
        assertArrayEquals(new byte[]{0x02, 0x03}, vec.get(1));
        assertTrue(vec.isNull(2));
        assertArrayEquals(new byte[]{(byte) 0xff}, vec.get(3));
        assertEquals(1, vec.getNullCount());
      }
    }
  }

  @Test
  public void testBool() throws Exception {
    Path file = findLanceFile("test_bool");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        BitVector vec = (BitVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(5, vec.getValueCount());
        assertEquals(1, vec.get(0));
        assertEquals(0, vec.get(1));
        assertTrue(vec.isNull(2));
        assertEquals(1, vec.get(3));
        assertEquals(0, vec.get(4));
        assertEquals(1, vec.getNullCount());
      }
    }
  }

  @Test
  public void testFixedSizeList() throws Exception {
    Path file = findLanceFile("test_fixed_size_list");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        FixedSizeListVector vec = (FixedSizeListVector) root.getVector("val");
        assertNotNull(vec);
        assertEquals(4, vec.getValueCount());

        // Row 0: [1.0, 2.0, 3.0]
        assertFalse(vec.isNull(0));
        @SuppressWarnings("unchecked")
        java.util.List<Double> row0 = (java.util.List<Double>) vec.getObject(0);
        assertEquals(java.util.List.of(1.0, 2.0, 3.0), row0);

        // Row 1: [4.0, 5.0, 6.0]
        assertFalse(vec.isNull(1));
        @SuppressWarnings("unchecked")
        java.util.List<Double> row1 = (java.util.List<Double>) vec.getObject(1);
        assertEquals(java.util.List.of(4.0, 5.0, 6.0), row1);

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: [7.0, 8.0, 9.0]
        assertFalse(vec.isNull(3));
        @SuppressWarnings("unchecked")
        java.util.List<Double> row3 = (java.util.List<Double>) vec.getObject(3);
        assertEquals(java.util.List.of(7.0, 8.0, 9.0), row3);

        assertEquals(1, vec.getNullCount());
      }
    }
  }
}
