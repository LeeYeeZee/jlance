package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 35: V2.1 RLE pattern (highly repetitive data).
 */
public class Milestone35CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_35");

  @Test
  public void testRle() throws Exception {
    Path file = DATA_DIR.resolve("test_rle.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(20000, root.getRowCount());

        BigIntVector rleInt64 = (BigIntVector) root.getVector("rle_int64");
        assertEquals(20000, rleInt64.getValueCount());

        Float8Vector rleFloat64 = (Float8Vector) root.getVector("rle_float64");
        assertEquals(20000, rleFloat64.getValueCount());

        BigIntVector rleBoolLike = (BigIntVector) root.getVector("rle_bool_like");
        assertEquals(20000, rleBoolLike.getValueCount());
        for (int i = 0; i < 20000; i++) {
          long v = rleBoolLike.get(i);
          assertTrue(v == 0L || v == 1L, "rle_bool_like should be 0 or 1, got " + v + " at " + i);
        }
      }
    }
  }
}
