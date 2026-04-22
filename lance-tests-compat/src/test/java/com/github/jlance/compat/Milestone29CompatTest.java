// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 29: LargeList of Struct.
 */
public class Milestone29CompatTest {

  private static Path dataPath(String name) {
    return Paths.get("..", "compat_tests", "data", "milestone_29", name);
  }

  @Test
  public void testLargeListOfStruct() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_large_list_struct.lance"))) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(4, root.getRowCount());
        LargeListVector vec = (LargeListVector) root.getVector("large_list_struct");
        StructVector dataVec = (StructVector) vec.getDataVector();
        IntVector xVec = (IntVector) dataVec.getChild("x");
        VarCharVector yVec = (VarCharVector) dataVec.getChild("y");

        // Row 0: [{x:1,y:"a"}, {x:2,y:"b"}]
        int start0 = (int) vec.getElementStartIndex(0);
        assertEquals(2, (int) vec.getElementEndIndex(0) - start0);
        assertEquals(1, xVec.get(start0));
        assertEquals(new Text("a"), yVec.getObject(start0));
        assertEquals(2, xVec.get(start0 + 1));
        assertEquals(new Text("b"), yVec.getObject(start0 + 1));

        // Row 1: [{x:3,y:"c"}]
        int start1 = (int) vec.getElementStartIndex(1);
        assertEquals(1, (int) vec.getElementEndIndex(1) - start1);
        assertEquals(3, xVec.get(start1));
        assertEquals(new Text("c"), yVec.getObject(start1));

        // Row 2: null
        assertTrue(vec.isNull(2));

        // Row 3: [{x:4,y:"d"}, {x:5,y:"e"}, {x:6,y:"f"}]
        int start3 = (int) vec.getElementStartIndex(3);
        assertEquals(3, (int) vec.getElementEndIndex(3) - start3);
        assertEquals(4, xVec.get(start3));
        assertEquals(new Text("d"), yVec.getObject(start3));
        assertEquals(5, xVec.get(start3 + 1));
        assertEquals(new Text("e"), yVec.getObject(start3 + 1));
        assertEquals(6, xVec.get(start3 + 2));
        assertEquals(new Text("f"), yVec.getObject(start3 + 2));
      }
    }
  }
}
