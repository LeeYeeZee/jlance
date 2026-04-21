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
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

public class Milestone12CompatTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_12", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void testZstdSinglePage() throws Exception {
    Path file = findLanceFile("test_zstd_single_page");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(5, root.getRowCount());

        IntVector intVec = (IntVector) root.getVector("int32_col");
        assertEquals(1, intVec.get(0));
        assertEquals(2, intVec.get(1));
        assertEquals(3, intVec.get(2));
        assertTrue(intVec.isNull(3));
        assertEquals(5, intVec.get(4));

        VarCharVector strVec = (VarCharVector) root.getVector("str_col");
        assertEquals(new Text("a"), strVec.getObject(0));
        assertEquals(new Text("bb"), strVec.getObject(1));
        assertTrue(strVec.isNull(2));
        assertEquals(new Text("dddd"), strVec.getObject(3));
        assertEquals(new Text("e"), strVec.getObject(4));
      }
    }
  }

  @Test
  public void testZstdMultiPageStruct() throws Exception {
    Path file = findLanceFile("test_zstd_multi_page_struct");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(100_000, root.getRowCount());

        StructVector structVec = (StructVector) root.getVector("s");
        assertFalse(structVec.isNull(0));
        assertFalse(structVec.isNull(99999));

        IntVector xVec = (IntVector) structVec.getChild("x");
        assertNotNull(xVec.getObject(0));
      }
    }
  }
}
