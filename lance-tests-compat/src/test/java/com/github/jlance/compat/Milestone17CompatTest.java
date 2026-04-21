package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone17CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_17", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testFileReaderProjectSingleColumn() throws Exception {
    Path file = findLanceFile("test_column_projection");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, Collections.singletonList("id"))) {
        assertEquals(50, root.getRowCount());
        assertEquals(1, root.getSchema().getFields().size());
        assertEquals("id", root.getSchema().getFields().get(0).getName());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(49, idVec.get(49));
      }
    }
  }

  @Test
  public void testFileReaderProjectMultipleColumns() throws Exception {
    Path file = findLanceFile("test_column_projection");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root =
          reader.readBatch(allocator, Arrays.asList("name", "score"))) {
        assertEquals(50, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertEquals("name", root.getSchema().getFields().get(0).getName());
        assertEquals("score", root.getSchema().getFields().get(1).getName());

        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals("row_000", nameVec.getObject(0).toString());

        Float8Vector scoreVec = (Float8Vector) root.getVector("score");
        assertFalse(Double.isNaN(scoreVec.get(0)));
      }
    }
  }

  @Test
  public void testFileReaderProjectEmptyList() throws Exception {
    Path file = findLanceFile("test_column_projection");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, Collections.emptyList())) {
        assertEquals(50, root.getRowCount());
        assertEquals(4, root.getSchema().getFields().size());
      }
    }
  }

  @Test
  public void testDatasetReaderProjectColumns() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_17", "test_column_projection");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(allocator, Arrays.asList("flag", "id"))) {
        assertEquals(50, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("flag"));
        assertNotNull(root.getVector("id"));
        assertNull(root.getVector("name"));
        assertNull(root.getVector("score"));
      }
    }
  }

  @Test
  public void testDatasetReaderProjectNull() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_17", "test_column_projection");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, (List<String>) null)) {
        assertEquals(50, root.getRowCount());
        assertEquals(4, root.getSchema().getFields().size());
      }
    }
  }
}
