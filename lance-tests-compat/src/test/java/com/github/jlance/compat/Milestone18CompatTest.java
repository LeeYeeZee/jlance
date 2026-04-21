package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone18CompatTest {

  @Test
  public void testDeletedRowsExcluded() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_18", "test_deletions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(15, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        // Deleted rows: 1, 3, 5, 7, 9
        assertEquals(0, idVec.get(0));
        assertEquals(2, idVec.get(1));
        assertEquals(4, idVec.get(2));
        assertEquals(6, idVec.get(3));
        assertEquals(8, idVec.get(4));
        assertEquals(10, idVec.get(5));
        assertEquals(19, idVec.get(14));

        // Verify no deleted IDs exist
        for (int i = 0; i < root.getRowCount(); i++) {
          int id = idVec.get(i);
          assertNotEquals(1, id);
          assertNotEquals(3, id);
          assertNotEquals(5, id);
          assertNotEquals(7, id);
          assertNotEquals(9, id);
        }
      }
    }
  }

  @Test
  public void testDeletedRowsWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_18", "test_deletions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(allocator, java.util.Collections.singletonList("name"))) {
        assertEquals(15, root.getRowCount());
        assertEquals(1, root.getSchema().getFields().size());

        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals("row_000", nameVec.getObject(0).toString());
        assertEquals("row_002", nameVec.getObject(1).toString());
      }
    }
  }
}
