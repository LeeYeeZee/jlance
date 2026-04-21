package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone23CompatTest {

  @Test
  public void testTakeSingleFragment() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.take(allocator, 1L, new int[] {0, 50, 99})) {
        assertEquals(3, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(50, idVec.get(1));
        assertEquals(99, idVec.get(2));
      }
    }
  }

  @Test
  public void testTakeWithDeletions() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      // Version 2 deleted rows with id < 30, leaving 70 visible rows (id 30..99).
      try (VectorSchemaRoot root = reader.take(allocator, 2L, new int[] {0, 34, 69})) {
        assertEquals(3, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(30, idVec.get(0));
        assertEquals(64, idVec.get(1));
        assertEquals(99, idVec.get(2));
      }
    }
  }

  @Test
  public void testTakeCrossFragment() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      // Version 3: fragment 0 has 70 visible rows (indices 0..69),
      // after deleting id<30. fragment 1 has 50 visible rows (indices 70..119).
      try (VectorSchemaRoot root =
          reader.take(allocator, 3L, new int[] {0, 69, 70, 119})) {
        assertEquals(4, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(30, idVec.get(0));   // first visible row of fragment 0
        assertEquals(99, idVec.get(1));   // last visible row of fragment 0
        assertEquals(100, idVec.get(2));  // first row of fragment 1
        assertEquals(149, idVec.get(3));  // last row of fragment 1
      }
    }
  }

  @Test
  public void testTakeWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.take(allocator, 3L, new int[] {10, 20}, Arrays.asList("id", "name"))) {
        assertEquals(2, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNotNull(root.getVector("name"));
        assertNull(root.getVector("score"));

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(40, idVec.get(0));  // global idx 10 -> local idx 10 -> id=40
        assertEquals(50, idVec.get(1));  // global idx 20 -> local idx 20 -> id=50
      }
    }
  }

  @Test
  public void testTakeEmptyIndices() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.take(allocator, 3L, new int[] {})) {
        assertEquals(0, root.getRowCount());
        assertEquals(3, root.getSchema().getFields().size());
      }
    }
  }

  @Test
  public void testTakeOutOfBounds() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      assertThrows(
          IndexOutOfBoundsException.class,
          () -> reader.take(allocator, 3L, new int[] {120}));
    }
  }
}
