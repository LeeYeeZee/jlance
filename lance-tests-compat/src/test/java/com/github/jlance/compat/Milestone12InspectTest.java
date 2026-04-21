package com.github.jlance.compat;

import com.github.jlance.format.LanceFileReader;
import com.github.jlance.format.LanceFileFooter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.file.v2.File2.ColumnMetadata;
import lance.encodings.EncodingsV20.ArrayEncoding;
import org.junit.jupiter.api.Test;

public class Milestone12InspectTest {

  private static Path findLanceFile(String name) throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_12", name, "data");
    try (var stream = Files.list(base)) {
      return stream.filter(p -> p.toString().endsWith(".lance")).findFirst().orElseThrow();
    }
  }

  @Test
  public void inspectZstdSinglePage() throws Exception {
    Path file = findLanceFile("test_zstd_single_page");
    try (LanceFileReader reader = new LanceFileReader(file)) {
      LanceFileFooter footer = reader.readFooter();
      for (int i = 0; i < footer.getColumnMetadatas().size(); i++) {
        ColumnMetadata cm = footer.getColumnMetadatas().get(i);
        System.out.println("Column " + i + ":");
        for (ColumnMetadata.Page page : cm.getPagesList()) {
          System.out.println("  page length=" + page.getLength() + " buffers=" + page.getBufferOffsetsCount());
          ArrayEncoding enc = com.github.jlance.format.decoder.PageDecoder.unpackArrayEncoding(page.getEncoding());
          if (enc != null) {
            System.out.println("    encoding case: " + enc.getArrayEncodingCase());
            if (enc.hasBinary()) {
              var binary = enc.getBinary();
              System.out.println("    binary null_adjustment=" + binary.getNullAdjustment());
              inspectFlat("indices", binary.getIndices());
              inspectFlat("bytes", binary.getBytes());
            }
            if (enc.hasNullable()) {
              var nullable = enc.getNullable();
              if (nullable.hasNoNulls()) {
                System.out.println("    nullable: NoNulls");
                inspectFlat("values", nullable.getNoNulls().getValues());
              }
              if (nullable.hasSomeNulls()) {
                System.out.println("    nullable: SomeNulls");
                inspectFlat("validity", nullable.getSomeNulls().getValidity());
                inspectFlat("values", nullable.getSomeNulls().getValues());
              }
            }
          }
        }
      }
    }
  }

  private void inspectFlat(String label, ArrayEncoding encoding) {
    if (encoding.hasFlat()) {
      var flat = encoding.getFlat();
      String scheme = flat.hasCompression() ? flat.getCompression().getScheme() : "none";
      System.out.println("      " + label + " flat bits=" + flat.getBitsPerValue() +
          " compression=" + flat.hasCompression() + " scheme=" + scheme);
    } else if (encoding.hasNullable()) {
      var nullable = encoding.getNullable();
      if (nullable.hasNoNulls()) {
        inspectFlat(label, nullable.getNoNulls().getValues());
      }
    }
  }
}
