package com.github.jlance.compat;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.jlance.format.LanceFileFooter;
import com.github.jlance.format.LanceFileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.file.v2.File2.ColumnMetadata;
import org.junit.jupiter.api.Test;

public class Milestone01CompatTest {

  @Test
  public void testReadLanceV2Footer() throws Exception {
    Path base = Paths.get("..", "compat_tests", "data", "milestone_01", "test.lance", "data");
    // Find the .lance data file (there should be exactly one)
    Path[] files;
    try (var stream = java.nio.file.Files.list(base)) {
      files = stream.filter(p -> p.toString().endsWith(".lance")).toArray(Path[]::new);
    }
    assertThat(files).hasSize(1);
    Path lanceFile = files[0];

    try (LanceFileReader reader = new LanceFileReader(lanceFile)) {
      LanceFileFooter footer = reader.readFooter();

      // Lance V2.0 files may have footer version (0, 3) or (2, 0)
      assertThat(footer.getMajorVersion()).isIn(0, 2);
      if (footer.getMajorVersion() == 0) {
        assertThat(footer.getMinorVersion()).isEqualTo(3);
      } else {
        assertThat(footer.getMinorVersion()).isEqualTo(0);
      }

      // Schema has 2 columns: id and name
      assertThat(footer.getNumColumns()).isEqualTo(2);
      assertThat(footer.getColumnMetadataOffsets()).hasSize(2);
      assertThat(footer.getColumnMetadatas()).hasSize(2);

      // Each column metadata should have at least one page
      for (ColumnMetadata cm : footer.getColumnMetadatas()) {
        assertThat(cm.getPagesCount()).isGreaterThanOrEqualTo(1);
      }

      // File size sanity check
      assertThat(reader.getFileSize()).isGreaterThan(40L);
    }
  }
}
