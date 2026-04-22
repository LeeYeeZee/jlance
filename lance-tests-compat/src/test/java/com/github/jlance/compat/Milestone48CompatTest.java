// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.junit.jupiter.api.Test;

/**
 * Compatibility tests for V2.1 packed struct and blob support.
 */
public class Milestone48CompatTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_48");

  private static Path dataPath(String name) {
    return DATA_DIR.resolve(name);
  }

  @Test
  public void testPackedStructV21() throws Exception {
    try (BufferAllocator allocator = new RootAllocator();
        LanceFileReader reader = new LanceFileReader(dataPath("test_packed_struct_v21.lance"))) {
      var metadata = reader.readMetadata();
      assertEquals(20, metadata.getNumRows());

      try (VectorSchemaRoot root = reader.readBatch(allocator)) {
        assertEquals(1, root.getFieldVectors().size());

        FieldVector structVec = root.getVector("packed_struct");
        assertInstanceOf(StructVector.class, structVec);
        StructVector struct = (StructVector) structVec;
        assertEquals(20, struct.getValueCount());

        IntVector x = (IntVector) struct.getChild("x");
        Float8Vector y = (Float8Vector) struct.getChild("y");

        for (int i = 0; i < 20; i++) {
          assertEquals(i, x.get(i), "x[" + i + "]");
          assertEquals(i * 1.5, y.get(i), 1e-9, "y[" + i + "]");
        }
      }
    }
  }
}
