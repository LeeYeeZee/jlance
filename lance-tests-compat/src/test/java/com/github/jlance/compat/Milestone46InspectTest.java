package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class Milestone46InspectTest {

  private static final Path DATA_DIR = Paths.get("..", "compat_tests", "data", "milestone_46");

  private void inspectFile(String name) throws Exception {
    Path file = DATA_DIR.resolve(name);
    if (!java.nio.file.Files.exists(file)) {
      org.junit.jupiter.api.Assumptions.assumeTrue(false, "Test data not generated: " + file);
    }

    try (LanceFileReader reader = new LanceFileReader(file)) {
      var footer = reader.readFooter();
      System.out.println("=== " + name + " ===");
      System.out.println("File version: " + footer.getMajorVersion() + "." + footer.getMinorVersion());
      System.out.println("Num columns: " + footer.getColumnMetadatas().size());

      var metadata = reader.readMetadata();
      System.out.println("Schema: " + metadata.getSchema());
      System.out.println("Row count: " + metadata.getNumRows());

      for (int c = 0; c < footer.getColumnMetadatas().size(); c++) {
        var col = footer.getColumnMetadatas().get(c);
        System.out.println("--- Column " + c + " ---");
        for (int p = 0; p < col.getPagesCount(); p++) {
          var page = col.getPages(p);
          System.out.println("  Page " + p + ": length=" + page.getLength()
              + " buffers=" + page.getBufferOffsetsCount());
          var layout = com.github.jlance.format.decoder.PageDecoder.unpackPageLayout(page.getEncoding());
          if (layout != null && layout.hasMiniBlockLayout()) {
            var mb = layout.getMiniBlockLayout();
            System.out.println("    MiniBlock: num_buffers=" + mb.getNumBuffers()
                + " num_items=" + mb.getNumItems()
                + " layers=" + mb.getLayersList());
            System.out.println("    hasDefCompression=" + mb.hasDefCompression());
            System.out.println("    hasRepCompression=" + mb.hasRepCompression());
          } else if (layout != null) {
            System.out.println("    Layout: " + layout.getLayoutCase());
          } else {
            var enc = com.github.jlance.format.decoder.PageDecoder.unpackArrayEncoding(page.getEncoding());
            System.out.println("    V2.0 Encoding: " + (enc != null ? enc.getArrayEncodingCase() : "null"));
          }
        }
      }
    }
  }

  @Test
  public void inspectBasic() throws Exception {
    inspectFile("test_struct_v21_basic.lance");
  }

  @Test
  public void inspectNullable() throws Exception {
    inspectFile("test_struct_v21_nullable.lance");
  }

  @Test
  public void inspectNullableChildren() throws Exception {
    inspectFile("test_struct_v21_nullable_children.lance");
  }
}
