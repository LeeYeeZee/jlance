// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

public class DebugM21Files {
  @Test
  public void debug() throws Exception {
    Path dataDir = Paths.get("..", "compat_tests", "data", "milestone_21", "test_schema_evolution", "data");
    try (BufferAllocator allocator = new RootAllocator()) {
      for (java.nio.file.Path p : java.nio.file.Files.newDirectoryStream(dataDir, "*.lance")) {
        System.out.println("=== " + p.getFileName() + " ===");
        try (LanceFileReader reader = new LanceFileReader(p)) {
          try (VectorSchemaRoot root = reader.readBatch(allocator)) {
            System.out.println("  rows: " + root.getRowCount());
            System.out.println("  fields: " + root.getSchema().getFields());
            for (int i = 0; i < root.getFieldVectors().size(); i++) {
              var vec = root.getFieldVectors().get(i);
              System.out.println("    " + vec.getName() + " -> " + vec.getClass().getSimpleName());
            }
          }
        }
      }
    }
  }
}
