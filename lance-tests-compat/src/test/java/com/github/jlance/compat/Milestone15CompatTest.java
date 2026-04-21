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

public class Milestone15CompatTest {

  @Test
  public void testSingleFragmentDataset() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_15", "test_single_fragment");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(4, root.getRowCount());

        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals("alice", nameVec.getObject(0).toString());
        assertEquals("bob", nameVec.getObject(1).toString());
        assertEquals("charlie", nameVec.getObject(2).toString());
        assertEquals("diana", nameVec.getObject(3).toString());

        Float8Vector scoreVec = (Float8Vector) root.getVector("score");
        assertEquals(10.5, scoreVec.get(0), 0.001);
        assertEquals(20.0, scoreVec.get(1), 0.001);
        assertEquals(15.3, scoreVec.get(2), 0.001);
        assertEquals(42.0, scoreVec.get(3), 0.001);
      }
    }
  }

  @Test
  public void testMultiFragmentDataset() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_15", "test_multi_fragment");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(5000, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertNotNull(idVec);
        assertFalse(idVec.isNull(0));
        assertFalse(idVec.isNull(4999));

        Float8Vector valueVec = (Float8Vector) root.getVector("value");
        assertNotNull(valueVec);
        assertFalse(valueVec.isNull(0));
        assertFalse(valueVec.isNull(4999));
      }
    }
  }
}
