package com.github.jlance.compat;

import com.github.jlance.format.LanceFileFooter;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.file.v2.File2.ColumnMetadata;
import org.junit.jupiter.api.Test;

public class Milestone05InspectTest {

  private static void inspect(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_05", name, "data");
    Path[] files;
    try (var stream = Files.list(base)) {
      files = stream.filter(p -> p.toString().endsWith(".lance")).toArray(Path[]::new);
    }
    Path lanceFile = files[0];

    try (LanceFileReader reader = new LanceFileReader(lanceFile)) {
      LanceFileFooter footer = reader.readFooter();
      System.out.println("=== " + name + " ===");
      System.out.println("Columns: " + footer.getNumColumns());
      for (int i = 0; i < footer.getNumColumns(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ": pages=" + cm.getPagesCount());
        for (int p = 0; p < cm.getPagesCount(); p++) {
          var page = cm.getPages(p);
          System.out.println("  Page " + p + ": length=" + page.getLength() + " buffers=" + page.getBufferOffsetsCount());
        }
      }
    }
  }

  @Test
  public void inspectAll() throws Exception {
    inspect("test_int32_multi_page");
    inspect("test_string_multi_page");
    inspect("test_struct_multi_page");
  }
}
