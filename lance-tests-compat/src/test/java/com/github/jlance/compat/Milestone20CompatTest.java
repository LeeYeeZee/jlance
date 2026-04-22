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
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class Milestone20CompatTest {

  @Test
  public void testRowIdsTwoLargeFragments() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        assertEquals(2000, root.getRowCount());
        assertNotNull(root.getVector("_rowid"));

        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");
        IntVector idVec = (IntVector) root.getVector("id");

        for (int i = 0; i < 1000; i++) {
          assertEquals(i, rowIdVec.get(i), "row ID mismatch at index " + i);
          assertEquals(i, idVec.get(i));
        }
        for (int i = 0; i < 1000; i++) {
          long expectedRowId = (1L << 32) | i;
          int idx = 1000 + i;
          assertEquals(expectedRowId, rowIdVec.get(idx), "row ID mismatch at index " + idx);
          assertEquals(1000 + i, idVec.get(idx));
        }
      }
    }
  }

  @Test
  public void testRowIdsWithProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(
              allocator, java.util.Collections.singletonList("id"), true)) {
        assertEquals(2000, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("id"));
        assertNotNull(root.getVector("_rowid"));

        IntVector idVec = (IntVector) root.getVector("id");
        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");

        assertEquals(0, idVec.get(0));
        assertEquals(0, rowIdVec.get(0));
        assertEquals(1999, idVec.get(1999));
        assertEquals((1L << 32) + 999, rowIdVec.get(1999));
      }
    }
  }

  @Test
  public void testWithoutRowId() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator)) {
        assertEquals(2000, root.getRowCount());
        assertEquals(3, root.getSchema().getFields().size());
        assertNull(root.getVector("_rowid"));
      }
    }
  }

  @Test
  public void testRowIdsAfterDeletions() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids_with_deletions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        assertEquals(900, root.getRowCount());
        assertNotNull(root.getVector("_rowid"));

        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");
        IntVector idVec = (IntVector) root.getVector("id");

        for (int i = 0; i < 100; i++) {
          assertEquals(i, rowIdVec.get(i));
          assertEquals(i, idVec.get(i));
        }
        for (int i = 100; i < 900; i++) {
          assertEquals(i + 100, idVec.get(i));
          assertEquals(i, rowIdVec.get(i));
        }
      }
    }
  }

  @Test
  public void testRowIdsAtFragmentBoundary() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");
        assertEquals(999, rowIdVec.get(999));
        assertEquals(1L << 32, rowIdVec.get(1000));
      }
    }
  }

  @Test
  public void testRowIdsMultiFragmentWithDeletions() throws Exception {
    Path datasetPath =
        Paths.get(
            "..", "compat_tests", "data", "milestone_20", "test_rowids_multi_frag_with_deletions");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        assertEquals(1900, root.getRowCount());
        assertNotNull(root.getVector("_rowid"));

        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");
        IntVector idVec = (IntVector) root.getVector("id");

        // Fragment 0: 1000 - 100 deleted = 900 rows
        // id 0..99 preserved, id 100..199 deleted, id 200..999 preserved
        for (int i = 0; i < 100; i++) {
          assertEquals(i, idVec.get(i));
          assertEquals(i, rowIdVec.get(i));
        }
        for (int i = 100; i < 900; i++) {
          assertEquals(i + 100, idVec.get(i));
          assertEquals(i, rowIdVec.get(i));
        }

        // Fragment 1: 1000 rows appended, no deletions
        // Local indices 0..999, row IDs start at 1<<32
        for (int i = 0; i < 1000; i++) {
          int idx = 900 + i;
          assertEquals(1000 + i, idVec.get(idx));
          long expectedRowId = (1L << 32) | i;
          assertEquals(expectedRowId, rowIdVec.get(idx));
        }
      }
    }
  }

  @Test
  public void testRowIdsAllDeleted() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids_all_deleted");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        assertEquals(0, root.getRowCount());
        assertNotNull(root.getVector("_rowid"));
        assertEquals(0, root.getVector("_rowid").getValueCount());
      }
    }
  }

  @Test
  public void testRowIdsEmptyDataset() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids_empty");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root = reader.readDataset(allocator, null, true)) {
        assertEquals(0, root.getRowCount());
        assertNotNull(root.getVector("_rowid"));
        assertEquals(0, root.getVector("_rowid").getValueCount());
        assertEquals(0, root.getVector("id").getValueCount());
      }
    }
  }

  @Test
  public void testRowIdsWithVersionAndProjection() throws Exception {
    Path datasetPath =
        Paths.get("..", "compat_tests", "data", "milestone_20", "test_rowids");
    try (BufferAllocator allocator = new RootAllocator();
        LanceDatasetReader reader = new LanceDatasetReader(datasetPath)) {
      try (VectorSchemaRoot root =
          reader.readDataset(
              allocator, 1L, java.util.Collections.singletonList("name"), true)) {
        assertEquals(1000, root.getRowCount());
        assertEquals(2, root.getSchema().getFields().size());
        assertNotNull(root.getVector("name"));
        assertNotNull(root.getVector("_rowid"));

        UInt8Vector rowIdVec = (UInt8Vector) root.getVector("_rowid");
        assertEquals(0, rowIdVec.get(0));
        assertEquals(999, rowIdVec.get(999));
      }
    }
  }
}
