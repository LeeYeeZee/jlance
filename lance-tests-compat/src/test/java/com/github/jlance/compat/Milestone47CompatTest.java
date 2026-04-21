package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.junit.jupiter.api.Test;

public class Milestone47CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_47");

  private static Path findFile(String name) {
    Path file = DATA_DIR.resolve(name + ".lance");
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(file), "Test data not found: " + file);
    return file;
  }

  @Test
  public void testListBasic() throws Exception {
    Path file = findFile("test_list_v21_basic");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);

        // Row 0: [1, 2, 3]
        assertFalse(vec.isNull(0));
        assertEquals(3, vec.getElementEndIndex(0) - vec.getElementStartIndex(0));
        IntVector items = (IntVector) vec.getDataVector();
        assertEquals(1, items.get(vec.getElementStartIndex(0)));
        assertEquals(2, items.get(vec.getElementStartIndex(0) + 1));
        assertEquals(3, items.get(vec.getElementStartIndex(0) + 2));

        // Row 1: [4, 5]
        assertFalse(vec.isNull(1));
        assertEquals(2, vec.getElementEndIndex(1) - vec.getElementStartIndex(1));
        assertEquals(4, items.get(vec.getElementStartIndex(1)));
        assertEquals(5, items.get(vec.getElementStartIndex(1) + 1));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3));

        // Row 4: [6]
        assertFalse(vec.isNull(4));
        assertEquals(1, vec.getElementEndIndex(4) - vec.getElementStartIndex(4));
        assertEquals(6, items.get(vec.getElementStartIndex(4)));
      }
    }
  }

  @Test
  public void testListNullable() throws Exception {
    Path file = findFile("test_list_v21_nullable");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);

        // Row 0: [1, 2]
        assertFalse(vec.isNull(0));
        assertEquals(2, vec.getElementEndIndex(0) - vec.getElementStartIndex(0));

        // Row 1: null
        assertTrue(vec.isNull(1));

        // Row 2: [3, 4, 5]
        assertFalse(vec.isNull(2));
        assertEquals(3, vec.getElementEndIndex(2) - vec.getElementStartIndex(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3));

        // Row 4: [6]
        assertFalse(vec.isNull(4));
        assertEquals(1, vec.getElementEndIndex(4) - vec.getElementStartIndex(4));
      }
    }
  }

  @Test
  public void testListNullableItems() throws Exception {
    Path file = findFile("test_list_v21_nullable_items");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        IntVector items = (IntVector) vec.getDataVector();

        // Row 0: [1, null, 3]
        assertFalse(vec.isNull(0));
        int off0 = vec.getElementStartIndex(0);
        assertEquals(1, items.get(off0));
        assertTrue(items.isNull(off0 + 1));
        assertEquals(3, items.get(off0 + 2));

        // Row 1: [null, 5]
        assertFalse(vec.isNull(1));
        int off1 = vec.getElementStartIndex(1);
        assertTrue(items.isNull(off1));
        assertEquals(5, items.get(off1 + 1));

        // Row 2: [6]
        assertFalse(vec.isNull(2));
        int off2 = vec.getElementStartIndex(2);
        assertEquals(6, items.get(off2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3));

        // Row 4: [7, 8]
        assertFalse(vec.isNull(4));
        int off4 = vec.getElementStartIndex(4);
        assertEquals(7, items.get(off4));
        assertEquals(8, items.get(off4 + 1));
      }
    }
  }

  @Test
  public void testListMultiPage() throws Exception {
    Path file = findFile("test_list_v21_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100000, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        IntVector items = (IntVector) vec.getDataVector();

        // Each row has 2 items: [i, i+1]
        for (int i = 0; i < 100000; i++) {
          assertFalse(vec.isNull(i), "Row " + i + " should not be null");
          int start = vec.getElementStartIndex(i);
          int end = vec.getElementEndIndex(i);
          assertEquals(2, end - start, "Row " + i + " should have 2 items");
          assertEquals(i, items.get(start), "Row " + i + " first item");
          assertEquals(i + 1, items.get(start + 1), "Row " + i + " second item");
        }
      }
    }
  }

  @Test
  public void testLargeListBasic() throws Exception {
    Path file = findFile("test_largelist_v21_basic");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        LargeListVector vec = (LargeListVector) root.getVector("val");
        assertNotNull(vec);
        BigIntVector items = (BigIntVector) vec.getDataVector();

        // Row 0: [1, 2, 3]
        assertFalse(vec.isNull(0));
        long off0 = vec.getElementStartIndex(0);
        assertEquals(1L, items.get((int) off0));
        assertEquals(2L, items.get((int) off0 + 1));
        assertEquals(3L, items.get((int) off0 + 2));

        // Row 1: [4, 5]
        assertFalse(vec.isNull(1));
        long off1 = vec.getElementStartIndex(1);
        assertEquals(4L, items.get((int) off1));
        assertEquals(5L, items.get((int) off1 + 1));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3));

        // Row 4: [6]
        assertFalse(vec.isNull(4));
        long off4 = vec.getElementStartIndex(4);
        assertEquals(6L, items.get((int) off4));
      }
    }
  }

  @Test
  public void testListString() throws Exception {
    Path file = findFile("test_list_v21_string");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("val");
        assertNotNull(vec);
        VarCharVector items = (VarCharVector) vec.getDataVector();

        // Row 0: ["a", "bb"]
        assertFalse(vec.isNull(0));
        int off0 = vec.getElementStartIndex(0);
        assertEquals(2, vec.getElementEndIndex(0) - off0);
        assertEquals("a", new String(items.get(off0), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("bb", new String(items.get(off0 + 1), java.nio.charset.StandardCharsets.UTF_8));

        // Row 1: ["ccc"]
        assertFalse(vec.isNull(1));
        int off1 = vec.getElementStartIndex(1);
        assertEquals(1, vec.getElementEndIndex(1) - off1);
        assertEquals("ccc", new String(items.get(off1), java.nio.charset.StandardCharsets.UTF_8));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: []
        assertFalse(vec.isNull(3));
        assertEquals(0, vec.getElementEndIndex(3) - vec.getElementStartIndex(3));

        // Row 4: ["d", "ee", "fff"]
        assertFalse(vec.isNull(4));
        int off4 = vec.getElementStartIndex(4);
        assertEquals(3, vec.getElementEndIndex(4) - off4);
        assertEquals("d", new String(items.get(off4), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("ee", new String(items.get(off4 + 1), java.nio.charset.StandardCharsets.UTF_8));
        assertEquals("fff", new String(items.get(off4 + 2), java.nio.charset.StandardCharsets.UTF_8));
      }
    }
  }
}
