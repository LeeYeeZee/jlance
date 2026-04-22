// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import org.apache.arrow.vector.types.pojo.Schema;

/**
 * High-level metadata for a Lance file, including the Arrow schema and row count.
 */
public class LanceFileMetadata {
  private final Schema schema;
  private final long numRows;

  public LanceFileMetadata(Schema schema, long numRows) {
    this.schema = schema;
    this.numRows = numRows;
  }

  public Schema getSchema() {
    return schema;
  }

  public long getNumRows() {
    return numRows;
  }
}
