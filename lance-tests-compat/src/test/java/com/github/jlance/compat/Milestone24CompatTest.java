package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone24CompatTest {

  @Test
  public void testHeadSingleFragment() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.head(allocator, 1L, 10)) {
        assertEquals(10, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(9, idVec.get(9));
      }
    }
  }

  @Test
  public void testHeadAcrossFragments() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      // Version 3: fragment 0 has 70 visible rows, fragment 1 has 50 visible rows.
      // head(80) should span both fragments.
      try (VectorSchemaRoot root = reader.head(allocator, 3L, 80)) {
        assertEquals(80, root.getRowCount());
        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(30, idVec.get(0));   // first visible row of fragment 0
        assertEquals(99, idVec.get(69));  // last visible row of fragment 0
        assertEquals(100, idVec.get(70)); // first row of fragment 1
        assertEquals(109, idVec.get(79)); // 10th row of fragment 1
      }
    }
  }

  @Test
  public void testHeadLargerThanTotal() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      // Version 2 has 70 visible rows. head(100) should return all 70.
      try (VectorSchemaRoot root = reader.head(allocator, 2L, 100)) {
        assertEquals(70, root.getRowCount());
      }
    }
  }

  @Test
  public void testHeadWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.head(allocator, 3L, 5, Arrays.asList("id", "name"))) {
        assertEquals(5, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNotNull(root.getVector("name"));
        assertNull(root.getVector("score"));

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(30, idVec.get(0));
        assertEquals(34, idVec.get(4));
      }
    }
  }

  @Test
  public void testHeadEmptyDataset() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_empty");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.head(allocator, 10)) {
        assertEquals(0, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
      }
    }
  }

  @Test
  public void testHeadZero() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_22", "test_count_rows");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.head(allocator, 3L, 0)) {
        assertEquals(0, root.getRowCount());
        assertEquals(3, root.getSchema().getFields().size());
      }
    }
  }
}
