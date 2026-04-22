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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

public class Milestone13CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_13", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testListOfStruct() throws Exception {
    Path file = findLanceFile("test_list_of_struct");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        ListVector listVec = (ListVector) root.getVector("list_struct");
        assertFalse(listVec.isNull(0));
        assertEquals(2, listVec.getElementEndIndex(0) - listVec.getElementStartIndex(0));
        assertFalse(listVec.isNull(1));
        assertEquals(1, listVec.getElementEndIndex(1) - listVec.getElementStartIndex(1));
        assertTrue(listVec.isNull(2));
        assertFalse(listVec.isNull(3));
        assertEquals(0, listVec.getElementEndIndex(3) - listVec.getElementStartIndex(3));
        assertFalse(listVec.isNull(4));
        assertEquals(3, listVec.getElementEndIndex(4) - listVec.getElementStartIndex(4));

        StructVector structVec = (StructVector) listVec.getDataVector();
        IntVector xVec = (IntVector) structVec.getChild("x");
        VarCharVector yVec = (VarCharVector) structVec.getChild("y");

        assertEquals(1, xVec.get(0));
        assertEquals(new Text("a"), yVec.getObject(0));
        assertEquals(2, xVec.get(1));
        assertEquals(new Text("b"), yVec.getObject(1));
        assertEquals(3, xVec.get(2));
        assertEquals(new Text("c"), yVec.getObject(2));
        assertEquals(4, xVec.get(3));
        assertEquals(new Text("d"), yVec.getObject(3));
        assertEquals(5, xVec.get(4));
        assertEquals(new Text("e"), yVec.getObject(4));
        assertEquals(6, xVec.get(5));
        assertEquals(new Text("f"), yVec.getObject(5));
      }
    }
  }

  @Test
  public void testStructOfList() throws Exception {
    Path file = findLanceFile("test_struct_of_list");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        StructVector structVec = (StructVector) root.getVector("struct_list");
        assertFalse(structVec.isNull(0));
        assertFalse(structVec.isNull(1));
        assertFalse(structVec.isNull(2));
        assertFalse(structVec.isNull(3));
        assertFalse(structVec.isNull(4));

        ListVector itemsVec = (ListVector) structVec.getChild("items");
        assertFalse(itemsVec.isNull(0));
        assertEquals(3, itemsVec.getElementEndIndex(0) - itemsVec.getElementStartIndex(0));
        assertFalse(itemsVec.isNull(1));
        assertEquals(0, itemsVec.getElementEndIndex(1) - itemsVec.getElementStartIndex(1));
        assertFalse(itemsVec.isNull(2));
        assertEquals(2, itemsVec.getElementEndIndex(2) - itemsVec.getElementStartIndex(2));
        assertTrue(itemsVec.isNull(3));
        assertFalse(itemsVec.isNull(4));
        assertEquals(1, itemsVec.getElementEndIndex(4) - itemsVec.getElementStartIndex(4));

        IntVector itemData = (IntVector) itemsVec.getDataVector();
        assertEquals(1, itemData.get(0));
        assertEquals(2, itemData.get(1));
        assertEquals(3, itemData.get(2));
        assertEquals(4, itemData.get(3));
        assertEquals(5, itemData.get(4));
        assertEquals(6, itemData.get(5));

        VarCharVector nameVec = (VarCharVector) structVec.getChild("name");
        assertEquals(new Text("foo"), nameVec.getObject(0));
        assertEquals(new Text("bar"), nameVec.getObject(1));
        assertTrue(nameVec.isNull(2));
        assertEquals(new Text("baz"), nameVec.getObject(3));
        assertEquals(new Text("qux"), nameVec.getObject(4));
      }
    }
  }

  @Test
  public void testListOfStructMultiPage() throws Exception {
    Path file = findLanceFile("test_list_of_struct_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(50_000, root.getRowCount());

        ListVector listVec = (ListVector) root.getVector("large_list_struct");
        assertTrue(listVec.isNull(0));  // i % 10 == 0
        assertFalse(listVec.isNull(1));
      }
    }
  }
}
