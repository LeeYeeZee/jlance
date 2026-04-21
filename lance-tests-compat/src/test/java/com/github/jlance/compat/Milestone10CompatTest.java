package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

public class Milestone10CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_10", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testDictionarySinglePage() throws Exception {
    Path file = findLanceFile("test_dictionary");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(7, root.getRowCount());

        VarCharVector dictStr = (VarCharVector) root.getVector("dict_str");
        assertEquals(new Text("a"), dictStr.getObject(0));
        assertEquals(new Text("b"), dictStr.getObject(1));
        assertEquals(new Text("a"), dictStr.getObject(2));
        assertEquals(new Text("c"), dictStr.getObject(3));
        assertTrue(dictStr.isNull(4));
        assertEquals(new Text("b"), dictStr.getObject(5));
        assertEquals(new Text("a"), dictStr.getObject(6));
      }
    }
  }

  @Test
  public void testDictionaryMultiPage() throws Exception {
    Path file = findLanceFile("test_dictionary_multi_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100_000, root.getRowCount());

        VarCharVector dictMulti = (VarCharVector) root.getVector("dict_multi");
        assertFalse(dictMulti.isNull(0));
        assertFalse(dictMulti.isNull(99999));
        assertEquals(new Text("alpha"), dictMulti.getObject(0));
        assertEquals(new Text("epsilon"), dictMulti.getObject(4));
      }
    }
  }
}
