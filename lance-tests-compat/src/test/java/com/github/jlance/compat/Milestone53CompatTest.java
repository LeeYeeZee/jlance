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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for list&lt;struct&lt;list&lt;struct&lt;int, string&gt;&gt;&gt;&gt;.
 */
public class Milestone53CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_53");

  @Test
  public void testListStructListStruct() throws Exception {
    Path file = DATA_DIR.resolve("test_list_struct_list_struct.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        ListVector outerList = (ListVector) root.getVector("entries");
        assertFalse(outerList.isNull(0));
        assertEquals(2, outerList.getElementEndIndex(0) - outerList.getElementStartIndex(0));
        assertFalse(outerList.isNull(1));
        assertEquals(1, outerList.getElementEndIndex(1) - outerList.getElementStartIndex(1));
        assertTrue(outerList.isNull(2));
        assertFalse(outerList.isNull(3));
        assertEquals(0, outerList.getElementEndIndex(3) - outerList.getElementStartIndex(3));
        assertFalse(outerList.isNull(4));
        assertEquals(1, outerList.getElementEndIndex(4) - outerList.getElementStartIndex(4));

        StructVector outerStruct = (StructVector) outerList.getDataVector();
        ListVector innerList = (ListVector) outerStruct.getChild("items");
        VarCharVector categoryVec = (VarCharVector) outerStruct.getChild("category");

        StructVector innerStruct = (StructVector) innerList.getDataVector();
        IntVector idVec = (IntVector) innerStruct.getChild("id");
        VarCharVector descVec = (VarCharVector) innerStruct.getChild("desc");

        // Outer struct 0: items=[{1,"a"},{2,"b"}], category="cat1"
        assertEquals(2, innerList.getElementEndIndex(0) - innerList.getElementStartIndex(0));
        assertEquals(1, idVec.get(0));
        assertEquals("a", descVec.getObject(0).toString());
        assertEquals(2, idVec.get(1));
        assertEquals("b", descVec.getObject(1).toString());
        assertEquals("cat1", categoryVec.getObject(0).toString());

        // Outer struct 1: items=[{3,"c"}], category="cat2"
        assertEquals(1, innerList.getElementEndIndex(1) - innerList.getElementStartIndex(1));
        assertEquals(3, idVec.get(2));
        assertEquals("c", descVec.getObject(2).toString());
        assertEquals("cat2", categoryVec.getObject(1).toString());

        // Outer struct 2: items=[], category="empty"
        assertEquals(0, innerList.getElementEndIndex(2) - innerList.getElementStartIndex(2));
        assertEquals("empty", categoryVec.getObject(2).toString());

        // Outer struct 3: items=[{4,"d"},{5,"e"},{6,"f"}], category="cat3"
        assertEquals(3, innerList.getElementEndIndex(3) - innerList.getElementStartIndex(3));
        assertEquals(4, idVec.get(3));
        assertEquals("d", descVec.getObject(3).toString());
        assertEquals(5, idVec.get(4));
        assertEquals("e", descVec.getObject(4).toString());
        assertEquals(6, idVec.get(5));
        assertEquals("f", descVec.getObject(5).toString());
        assertEquals("cat3", categoryVec.getObject(3).toString());
      }
    }
  }
}
