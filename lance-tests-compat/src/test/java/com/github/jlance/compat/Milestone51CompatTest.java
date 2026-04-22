// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility test for list&lt;struct&lt;list&lt;string&gt;&gt;&gt;.
 */
public class Milestone51CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_51");

  @Test
  public void testListStructListString() throws Exception {
    Path file = DATA_DIR.resolve("test_list_struct_list_string.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        ListVector outerList = (ListVector) root.getVector("docs");
        assertFalse(outerList.isNull(0));
        assertEquals(2, outerList.getElementEndIndex(0) - outerList.getElementStartIndex(0));
        assertFalse(outerList.isNull(1));
        assertEquals(1, outerList.getElementEndIndex(1) - outerList.getElementStartIndex(1));
        assertTrue(outerList.isNull(2));
        assertFalse(outerList.isNull(3));
        assertEquals(0, outerList.getElementEndIndex(3) - outerList.getElementStartIndex(3));
        assertFalse(outerList.isNull(4));
        assertEquals(1, outerList.getElementEndIndex(4) - outerList.getElementStartIndex(4));

        StructVector structVec = (StructVector) outerList.getDataVector();
        ListVector innerList = (ListVector) structVec.getChild("tags");
        VarCharVector labelVec = (VarCharVector) structVec.getChild("label");

        // Struct 0: tags=["a","bb"], label="x"
        assertEquals(2, innerList.getElementEndIndex(0) - innerList.getElementStartIndex(0));
        VarCharVector innerTags = (VarCharVector) innerList.getDataVector();
        assertEquals("a", innerTags.getObject(0).toString());
        assertEquals("bb", innerTags.getObject(1).toString());
        assertEquals("x", labelVec.getObject(0).toString());

        // Struct 1: tags=["ccc"], label="y"
        assertEquals(1, innerList.getElementEndIndex(1) - innerList.getElementStartIndex(1));
        assertEquals("ccc", innerTags.getObject(2).toString());
        assertEquals("y", labelVec.getObject(1).toString());

        // Struct 2: tags=[], label="empty_tags"
        assertEquals(0, innerList.getElementEndIndex(2) - innerList.getElementStartIndex(2));
        assertEquals("empty_tags", labelVec.getObject(2).toString());

        // Struct 3: tags=["hello","world","foo"], label="z"
        assertEquals(3, innerList.getElementEndIndex(3) - innerList.getElementStartIndex(3));
        assertEquals("hello", innerTags.getObject(3).toString());
        assertEquals("world", innerTags.getObject(4).toString());
        assertEquals("foo", innerTags.getObject(5).toString());
        assertEquals("z", labelVec.getObject(3).toString());
      }
    }
  }
}
