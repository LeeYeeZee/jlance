// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import java.util.Collections;
import java.util.List;
import lance.file.v2.File2.ColumnMetadata;

/**
 * Parsed Lance v2 file footer and metadata tables.
 */
public class LanceFileFooter {
  private final int majorVersion;
  private final int minorVersion;
  private final int numColumns;
  private final int numGlobalBuffers;
  private final long columnMetadataOffset;
  private final long cmoTableOffset;
  private final long gboTableOffset;
  private final List<ColumnMetadataOffset> columnMetadataOffsets;
  private final List<GlobalBufferOffset> globalBufferOffsets;
  private final List<ColumnMetadata> columnMetadatas;

  public LanceFileFooter(
      int majorVersion,
      int minorVersion,
      int numColumns,
      int numGlobalBuffers,
      long columnMetadataOffset,
      long cmoTableOffset,
      long gboTableOffset,
      List<ColumnMetadataOffset> columnMetadataOffsets,
      List<GlobalBufferOffset> globalBufferOffsets,
      List<ColumnMetadata> columnMetadatas) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
    this.numColumns = numColumns;
    this.numGlobalBuffers = numGlobalBuffers;
    this.columnMetadataOffset = columnMetadataOffset;
    this.cmoTableOffset = cmoTableOffset;
    this.gboTableOffset = gboTableOffset;
    this.columnMetadataOffsets = Collections.unmodifiableList(columnMetadataOffsets);
    this.globalBufferOffsets = Collections.unmodifiableList(globalBufferOffsets);
    this.columnMetadatas = Collections.unmodifiableList(columnMetadatas);
  }

  public int getMajorVersion() {
    return majorVersion;
  }

  public int getMinorVersion() {
    return minorVersion;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumGlobalBuffers() {
    return numGlobalBuffers;
  }

  public long getColumnMetadataOffset() {
    return columnMetadataOffset;
  }

  public long getCmoTableOffset() {
    return cmoTableOffset;
  }

  public long getGboTableOffset() {
    return gboTableOffset;
  }

  public List<ColumnMetadataOffset> getColumnMetadataOffsets() {
    return columnMetadataOffsets;
  }

  public List<GlobalBufferOffset> getGlobalBufferOffsets() {
    return globalBufferOffsets;
  }

  public List<ColumnMetadata> getColumnMetadatas() {
    return columnMetadatas;
  }

  /**
   * Returns true if this is a Lance v2.x file. Note that v2.0 files may have footer version
   * (0, 3) or (2, 0) due to historical reasons.
   */
  public boolean isV2() {
    return isV2_0() || isV2_1OrLater();
  }

  /** Returns true if this is a Lance v2.0 file. */
  public boolean isV2_0() {
    return (majorVersion == 0 && minorVersion == 3)
        || (majorVersion == 2 && minorVersion == 0);
  }

  /** Returns true if this is a Lance v2.1 file. */
  public boolean isV2_1() {
    return majorVersion == 2 && minorVersion == 1;
  }

  /** Returns true if this is a Lance v2.2 file. */
  public boolean isV2_2() {
    return majorVersion == 2 && minorVersion == 2;
  }

  /** Returns true if this is a Lance v2.1 or later file. */
  public boolean isV2_1OrLater() {
    return majorVersion == 2 && minorVersion >= 1;
  }

  @Override
  public String toString() {
    return "LanceFileFooter{"
        + "version="
        + majorVersion
        + "."
        + minorVersion
        + ", numColumns="
        + numColumns
        + ", numGlobalBuffers="
        + numGlobalBuffers
        + '}';
  }

  public record ColumnMetadataOffset(long position, long size) {}

  public record GlobalBufferOffset(long position, long size) {}
}
