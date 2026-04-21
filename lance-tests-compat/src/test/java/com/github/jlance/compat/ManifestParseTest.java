package com.github.jlance.compat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lance.table.Table.Manifest;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

public class ManifestParseTest {

  private static final byte[] MAGIC = new byte[] {'L', 'A', 'N', 'C'};

  @Test
  public void testParseManifest() throws Exception {
    Path manifestPath =
        Paths.get(
            "..",
            "compat_tests",
            "data",
            "milestone_08",
            "test_decimal",
            "_versions",
            "18446744073709551614.manifest");
    byte[] data = Files.readAllBytes(manifestPath);
    System.out.println("Manifest file size: " + data.length);

    Manifest manifest = readManifest(data);
    System.out.println("Parsed manifest!");
    System.out.println("Fields: " + manifest.getFieldsCount());
    System.out.println("Fragments: " + manifest.getFragmentsCount());
    System.out.println("Version: " + manifest.getVersion());

    assertThat(manifest.getFieldsCount()).isGreaterThan(0);
    assertThat(manifest.getFragmentsCount()).isGreaterThan(0);
  }

  /**
   * Reads a manifest file following the Lance format:
   * <pre>
   *   [protobuf data] [length: u32 LE] [manifest_pos: i64 LE] [magic: 8 bytes]
   * </pre>
   * The magic is 8 bytes starting with "LANC".
   */
  public static Manifest readManifest(byte[] data) throws Exception {
    if (data.length < 16) {
      throw new IllegalArgumentException("Manifest file too small: " + data.length);
    }

    // Verify magic at end of file
    boolean hasMagic = false;
    // Magic can be at different positions in the trailing 8 bytes.
    // In practice, the last 4 bytes are "LANC".
    for (int i = data.length - 4; i <= data.length - 4; i++) {
      if (data[i] == 'L'
          && data[i + 1] == 'A'
          && data[i + 2] == 'N'
          && data[i + 3] == 'C') {
        hasMagic = true;
        break;
      }
    }
    if (!hasMagic) {
      throw new IllegalArgumentException("Invalid manifest: magic number not found");
    }

    // manifest_pos is 8 bytes before the magic (or 8 bytes before end of file if magic is 4 bytes)
    // Based on Rust code: manifest_pos is read from buf[len-16..len-8]
    long manifestPos =
        ByteBuffer.wrap(data, data.length - 16, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
    int manifestLen = data.length - (int) manifestPos;

    ByteBuffer manifestBuf = ByteBuffer.wrap(data, (int) manifestPos, manifestLen);
    manifestBuf.order(ByteOrder.LITTLE_ENDIAN);

    // First 4 bytes are the recorded protobuf length
    int recordedLength = manifestBuf.getInt();

    // Protobuf data follows, excluding the 4-byte length prefix and the trailing 16 bytes (pos + magic)
    int protobufLen = manifestLen - 4 - 16;
    if (protobufLen != recordedLength) {
      throw new IllegalStateException(
          "Manifest length mismatch: recorded="
              + recordedLength
              + ", actual="
              + protobufLen);
    }

    byte[] protobufBytes = new byte[protobufLen];
    manifestBuf.get(protobufBytes);
    return Manifest.parseFrom(protobufBytes);
  }
}
