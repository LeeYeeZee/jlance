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

public class Milestone21CompatTest {

  @Test
  public void testReadVersion1OriginalSchema() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 1L)) {
        assertEquals(20, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNotNull(root.getVector("name"));
        assertNull(root.getVector("score"));

        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals(0, idVec.get(0));
        assertEquals("row_000", nameVec.getObject(0).toString());
        assertEquals(19, idVec.get(19));
        assertEquals("row_019", nameVec.getObject(19).toString());
      }
    }
  }

  @Test
  public void testReadVersion2WithAddedColumn() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 2L)) {
        assertEquals(20, root.getRowCount());
        assertEquals(3, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNotNull(root.getVector("name"));
        assertNotNull(root.getVector("score"));

        IntVector idVec = (IntVector) root.getVector("id");
        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        Float8Vector scoreVec = (Float8Vector) root.getVector("score");

        assertEquals(0, idVec.get(0));
        assertEquals("row_000", nameVec.getObject(0).toString());
        assertEquals(0.0, scoreVec.get(0), 1e-6);

        assertEquals(10, idVec.get(10));
        assertEquals("row_010", nameVec.getObject(10).toString());
        assertEquals(15.0, scoreVec.get(10), 1e-6);

        assertEquals(19, idVec.get(19));
        assertEquals(28.5, scoreVec.get(19), 1e-6);
      }
    }
  }

  @Test
  public void testReadVersion3WithDroppedColumn() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 3L)) {
        assertEquals(20, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNull(root.getVector("name"));
        assertNotNull(root.getVector("score"));

        IntVector idVec = (IntVector) root.getVector("id");
        Float8Vector scoreVec = (Float8Vector) root.getVector("score");

        assertEquals(0, idVec.get(0));
        assertEquals(0.0, scoreVec.get(0), 1e-6);
        assertEquals(19, idVec.get(19));
        assertEquals(28.5, scoreVec.get(19), 1e-6);
      }
    }
  }

  @Test
  public void testReadLatestDefaultsToVersion3() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(20, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNull(root.getVector("name"));
        assertNotNull(root.getVector("score"));
      }
    }
  }

  @Test
  public void testSchemaEvolutionWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(
              allocator, 2L, java.util.Collections.singletonList("score"))) {
        assertEquals(20, root.getRowCount());
        assertEquals(1, root.getSchema().getFields().size());
        assertNotNull(root.getVector("score"));
        assertNull(root.getVector("id"));
        assertNull(root.getVector("name"));

        Float8Vector scoreVec = (Float8Vector) root.getVector("score");
        assertEquals(15.0, scoreVec.get(10), 1e-6);
      }
    }
  }
}
