// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 27: List of Dictionary values.
 */
public class Milestone27CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_27", name);
  }

  @Test
  public void testListOfDictionary() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_list_dict.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());
        ListVector vec = (ListVector) root.getVector("list_dict");
        org.apache.arrow.vector.FieldVector dataVec = vec.getDataVector();
        VarCharVector itemVec = (VarCharVector) dataVec;

        // Row 0: ["a", "b"]
        int start0 = vec.getElementStartIndex(0);
        int end0 = vec.getElementEndIndex(0);
        assertEquals(2, end0 - start0);
        assertEquals(new Text("a"), itemVec.getObject(start0));
        assertEquals(new Text("b"), itemVec.getObject(start0 + 1));

        // Row 1: ["c"]
        int start1 = vec.getElementStartIndex(1);
        int end1 = vec.getElementEndIndex(1);
        assertEquals(1, end1 - start1);
        assertEquals(new Text("c"), itemVec.getObject(start1));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: ["a", "c", "b"]
        int start3 = vec.getElementStartIndex(3);
        int end3 = vec.getElementEndIndex(3);
        assertEquals(3, end3 - start3);
        assertEquals(new Text("a"), itemVec.getObject(start3));
        assertEquals(new Text("c"), itemVec.getObject(start3 + 1));
        assertEquals(new Text("b"), itemVec.getObject(start3 + 2));

        // Row 4: ["d"]
        int start4 = vec.getElementStartIndex(4);
        int end4 = vec.getElementEndIndex(4);
        assertEquals(1, end4 - start4);
        assertEquals(new Text("d"), itemVec.getObject(start4));
      }
    }
  }
}
