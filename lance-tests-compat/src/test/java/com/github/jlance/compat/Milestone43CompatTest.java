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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.Test;

public class Milestone43CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_43");

  private static Path findFile(String name) {
    Path file = DATA_DIR.resolve(name + ".lance");
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(file), "Test data not found: " + file);
    return file;
  }

  @Test
  public void testMixedNestedList() throws Exception {
    Path file = findFile("test_miniblock_nested");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("mixed_nested");
        assertNotNull(vec);

        for (int i = 0; i < 100; i++) {
          if (i % 4 == 0) {
            assertTrue(vec.isNull(i), "Row " + i + " should be null");
          } else {
            assertFalse(vec.isNull(i), "Row " + i + " should not be null");
            int outerStart = vec.getElementStartIndex(i);
            int outerEnd = vec.getElementEndIndex(i);

            if (i % 4 == 1) {
              assertEquals(1, outerEnd - outerStart, "Row " + i + " should have 1 inner list");
              ListVector innerVec = (ListVector) vec.getDataVector();
              int innerStart = innerVec.getElementStartIndex(outerStart);
              int innerEnd = innerVec.getElementEndIndex(outerStart);
              assertEquals(0, innerEnd - innerStart, "Inner list should be empty");
            } else if (i % 4 == 2) {
              assertEquals(1, outerEnd - outerStart, "Row " + i + " should have 1 inner list");
              ListVector innerVec = (ListVector) vec.getDataVector();
              int innerStart = innerVec.getElementStartIndex(outerStart);
              int innerEnd = innerVec.getElementEndIndex(outerStart);
              assertEquals(3, innerEnd - innerStart, "Inner list should have 3 items");
            } else {
              assertEquals(2, outerEnd - outerStart, "Row " + i + " should have 2 inner lists");
              ListVector innerVec = (ListVector) vec.getDataVector();
              int innerStart0 = innerVec.getElementStartIndex(outerStart);
              int innerEnd0 = innerVec.getElementEndIndex(outerStart);
              assertEquals(1, innerEnd0 - innerStart0, "First inner list should have 1 item");
              int innerStart1 = innerVec.getElementStartIndex(outerStart + 1);
              int innerEnd1 = innerVec.getElementEndIndex(outerStart + 1);
              assertEquals(2, innerEnd1 - innerStart1, "Second inner list should have 2 items");
            }
          }
        }
      }
    }
  }
}
