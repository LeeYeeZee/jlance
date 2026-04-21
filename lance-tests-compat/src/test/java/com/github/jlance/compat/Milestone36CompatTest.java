package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for Milestone 36: V2.1 high-entropy compressed data.
 */
public class Milestone36CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_36");

  @Test
  public void testHighEntropy() throws Exception {
    Path file = DATA_DIR.resolve("test_high_entropy.lance");
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(file)) {
      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(15000, root.getRowCount());

        BigIntVector int64Vec = (BigIntVector) root.getVector("high_entropy_int64");
        assertEquals(15000, int64Vec.getValueCount());

        Float8Vector float64Vec = (Float8Vector) root.getVector("high_entropy_float64");
        assertEquals(15000, float64Vec.getValueCount());

        IntVector int32Vec = (IntVector) root.getVector("high_entropy_int32");
        assertEquals(15000, int32Vec.getValueCount());
      }
    }
  }
}
