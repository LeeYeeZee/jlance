package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.LanceFileFooter;
import java.util.Collections;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LanceFileFooter} version detection methods.
 */
public class LanceFileFooterVersionTest {

  private static LanceFileFooter createFooter(int major, int minor) {
    return new LanceFileFooter(
        major,
        minor,
        1, // numColumns
        0, // numGlobalBuffers
        0L, // columnMetadataOffset
        0L, // cmoTableOffset
        0L, // gboTableOffset
        Collections.emptyList(), // columnMetadataOffsets
        Collections.emptyList(), // globalBufferOffsets
        Collections.emptyList()  // columnMetadatas
    );
  }

  @Test
  public void testV20Alias() {
    LanceFileFooter footer = createFooter(0, 3);
    assertTrue(footer.isV2());
    assertTrue(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertFalse(footer.isV2_1OrLater());
  }

  @Test
  public void testV20Explicit() {
    LanceFileFooter footer = createFooter(2, 0);
    assertTrue(footer.isV2());
    assertTrue(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertFalse(footer.isV2_1OrLater());
  }

  @Test
  public void testV21() {
    LanceFileFooter footer = createFooter(2, 1);
    assertTrue(footer.isV2());
    assertFalse(footer.isV2_0());
    assertTrue(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertTrue(footer.isV2_1OrLater());
  }

  @Test
  public void testV22() {
    LanceFileFooter footer = createFooter(2, 2);
    assertTrue(footer.isV2());
    assertFalse(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertTrue(footer.isV2_2());
    assertTrue(footer.isV2_1OrLater());
  }

  @Test
  public void testV23() {
    LanceFileFooter footer = createFooter(2, 3);
    assertTrue(footer.isV2());
    assertFalse(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertTrue(footer.isV2_1OrLater());
  }

  @Test
  public void testLegacy01() {
    LanceFileFooter footer = createFooter(0, 1);
    assertFalse(footer.isV2());
    assertFalse(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertFalse(footer.isV2_1OrLater());
  }

  @Test
  public void testFutureVersion() {
    LanceFileFooter footer = createFooter(2, 99);
    assertTrue(footer.isV2());
    assertFalse(footer.isV2_0());
    assertFalse(footer.isV2_1());
    assertFalse(footer.isV2_2());
    assertTrue(footer.isV2_1OrLater());
  }
}
