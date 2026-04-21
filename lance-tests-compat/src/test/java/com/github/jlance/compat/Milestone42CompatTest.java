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

public class Milestone42CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_42");

  private static Path findFile(String name) {
    Path file = DATA_DIR.resolve(name + ".lance");
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(file), "Test data not found: " + file);
    return file;
  }

  @Test
  public void testAllNullNestedList() throws Exception {
    Path file = findFile("test_constant_nested");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("all_null");
        assertNotNull(vec);

        for (int i = 0; i < 100; i++) {
          assertTrue(vec.isNull(i), "Row " + i + " should be null");
        }
      }
    }
  }

  @Test
  public void testAllEmptyNestedList() throws Exception {
    Path file = findFile("test_constant_nested");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("all_empty");
        assertNotNull(vec);

        for (int i = 0; i < 100; i++) {
          assertFalse(vec.isNull(i), "Row " + i + " should not be null");
          int start = vec.getElementStartIndex(i);
          int end = vec.getElementEndIndex(i);
          assertEquals(0, end - start, "Row " + i + " should have 0 inner lists");
        }
      }
    }
  }

  @Test
  public void testOneEmptyInnerNestedList() throws Exception {
    Path file = findFile("test_constant_nested");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("one_empty_inner");
        assertNotNull(vec);

        ListVector innerVec = (ListVector) vec.getDataVector();

        for (int i = 0; i < 100; i++) {
          assertFalse(vec.isNull(i), "Row " + i + " should not be null");
          int outerStart = vec.getElementStartIndex(i);
          int outerEnd = vec.getElementEndIndex(i);
          assertEquals(1, outerEnd - outerStart, "Row " + i + " should have 1 inner list");

          int innerStart = innerVec.getElementStartIndex(outerStart);
          int innerEnd = innerVec.getElementEndIndex(outerStart);
          assertEquals(0, innerEnd - innerStart, "Inner list should be empty");
        }
      }
    }
  }
}
