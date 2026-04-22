// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.BigIntVector;
import org.junit.jupiter.api.Test;

public class Milestone16CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_16", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testReadRangeMiddle() throws Exception {
    Path file = findLanceFile("test_row_range");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, 10, 20)) {
        assertEquals(20, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(10, idVec.get(0));
        assertEquals(29, idVec.get(19));

        Float8Vector valueVec = (Float8Vector) root.getVector("value");
        assertEquals(10.0 * 1.5, valueVec.get(0), 0.001);
        assertEquals(29.0 * 1.5, valueVec.get(19), 0.001);

        VarCharVector nameVec = (VarCharVector) root.getVector("name");
        assertEquals("row_010", nameVec.getObject(0).toString());
        assertEquals("row_029", nameVec.getObject(19).toString());
      }
    }
  }

  @Test
  public void testReadRangeStart() throws Exception {
    Path file = findLanceFile("test_row_range");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, 0, 5)) {
        assertEquals(5, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(0, idVec.get(0));
        assertEquals(4, idVec.get(4));
      }
    }
  }

  @Test
  public void testReadRangeEnd() throws Exception {
    Path file = findLanceFile("test_row_range");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, 90, 20)) {
        assertEquals(10, root.getRowCount());

        IntVector idVec = (IntVector) root.getVector("id");
        assertEquals(90, idVec.get(0));
        assertEquals(99, idVec.get(9));
      }
    }
  }

  @Test
  public void testReadRangeBeyondEnd() throws Exception {
    Path file = findLanceFile("test_row_range");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, 100, 10)) {
        assertEquals(0, root.getRowCount());
      }
    }
  }

  @Test
  public void testReadRangeNullable() throws Exception {
    Path file = findLanceFile("test_row_range");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator, 0, 10)) {
        assertEquals(10, root.getRowCount());

        BigIntVector nullableVec = (BigIntVector) root.getVector("nullable_int");
        // Rows 0, 5 are null (every 5th row)
        assertTrue(nullableVec.isNull(0));
        assertFalse(nullableVec.isNull(1));
        assertFalse(nullableVec.isNull(4));
        assertTrue(nullableVec.isNull(5));
        assertEquals(6L, nullableVec.get(6));
      }
    }
  }
}
