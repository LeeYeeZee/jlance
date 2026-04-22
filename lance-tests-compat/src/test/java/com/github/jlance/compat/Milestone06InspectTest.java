// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.compat;

import com.github.jlance.format.LanceFileFooter;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.file.v2.File2.ColumnMetadata;
import org.junit.jupiter.api.Test;

public class Milestone06InspectTest {
  private static void inspect(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_06", name, "data");
    Path[] files;
    try (var stream = Files.list(base)) {
      files = stream.filter(p -> p.toString().endsWith(".lance")).toArray(Path[]::new);
    }
    Path lanceFile = files[0];
    try (LanceFileReader reader = new LanceFileReader(lanceFile)) {
      LanceFileFooter footer = reader.readFooter();
      System.out.println("=== " + name + " ===");
      for (int i = 0; i < footer.getNumColumns(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ": pages=" + cm.getPagesCount());
        for (int p = 0; p < Math.min(5, cm.getPagesCount()); p++) {
          var page = cm.getPages(p);
          System.out.println("  Page " + p + ": length=" + page.getLength() + " buffers=" + page.getBufferOffsetsCount());
        }
        if (cm.getPagesCount() > 5) {
          System.out.println("  ...");
          for (int p = cm.getPagesCount() - 3; p < cm.getPagesCount(); p++) {
            var page = cm.getPages(p);
            System.out.println("  Page " + p + ": length=" + page.getLength() + " buffers=" + page.getBufferOffsetsCount());
          }
        }
      }
    }
  }

  @Test
  public void inspectAll() throws Exception {
    inspect("test_list_int32_multi_page");
  }
}
