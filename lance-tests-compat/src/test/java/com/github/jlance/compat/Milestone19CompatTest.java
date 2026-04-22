// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceDatasetReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone19CompatTest {

  @Test
  public void testReadVersion1() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_19", "test_versions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 1L)) {
        assertEquals(20, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(1, idVec.get(1));
        assertEquals(19, idVec.get(19));
      }
    }
  }

  @Test
  public void testReadVersion2WithDeletions() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_19", "test_versions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 2L)) {
        assertEquals(15, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(2, idVec.get(1));
        assertEquals(4, idVec.get(2));
        assertEquals(19, idVec.get(14));

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
  public void testReadVersion3WithAppend() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_19", "test_versions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, 3L)) {
        assertEquals(25, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        // First fragment: 15 rows (20 original minus 5 deleted)
        assertEquals(0, idVec.get(0));
        assertEquals(2, idVec.get(1));
        // Second fragment: 10 appended rows
        assertEquals(20, idVec.get(15));
        assertEquals(29, idVec.get(24));
      }
    }
  }

  @Test
  public void testReadLatestDefaultsToVersion3() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_19", "test_versions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(25, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(29, idVec.get(24));
      }
    }
  }

  @Test
  public void testReadVersionWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_19", "test_versions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(allocator, 2L, java.util.Collections.singletonList("name"))) {
        assertEquals(15, root.getRowCount());
        assertEquals(1, root.getSchema().getFields().size());

        org.apache.arrow.vector.VarCharVector nameVec =
            (org.apache.arrow.vector.VarCharVector) root.getVector("name");
        assertEquals("row_000", nameVec.getObject(0).toString());
        assertEquals("row_002", nameVec.getObject(1).toString());
      }
    }
  }
}
