// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.V21ListUnraveler;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a V2.1+ nested list column by recursively peeling off one list layer at a time.
 *
 * <p>This is the Java analog of the Rust {@code StructuralListDecodeTask} from the canonical
 * {@code lance-encoding} crate (see {@code lance-encoding/src/encodings/logical/list.rs}).
 *
 * <p>The task works in conjunction with {@link V21ListUnraveler} (the Java analog of Rust's
 * {@code RepDefUnraveler}). The unraveler consumes one list layer per call to
 * {@link V21ListUnraveler#unravelOffsets(int)}. The recursive order is <strong>bottom-up</strong>:
 * the innermost child vector is decoded first, and then each outer layer wraps it. This matches
 * the Rust design where {@code child_task.decode()} is called before
 * {@code repdef.unravel_offsets()}.
 */
public class StructuralListDecodeTask {

  private final V21ListUnraveler unraveler;
  private final FieldVector innerVec;
  private final Field field;
  private final int numRows;
  private final BufferAllocator allocator;

  /**
   * Creates a new decode task.
   *
   * @param unraveler  the rep/def unraveler (one layer will be consumed)
   * @param innerVec   the already-decoded inner vector (values or next list level)
   * @param field      the Arrow field for the current list layer
   * @param numRows    number of rows (lists) at this layer
   * @param allocator  Arrow buffer allocator
   */
  public StructuralListDecodeTask(
      V21ListUnraveler unraveler,
      FieldVector innerVec,
      Field field,
      int numRows,
      BufferAllocator allocator) {
    this.unraveler = unraveler;
    this.innerVec = innerVec;
    this.field = field;
    this.numRows = numRows;
    this.allocator = allocator;
  }

  /**
   * Decodes the current list layer and recurses into deeper layers if needed.
   *
   * <p>Execution order (matching Rust):
   * <ol>
   *   <li>Recurse into child task first (bottom-up)</li>
   *   <li>Unravel current list layer from the shared rep/def state</li>
   *   <li>Build the ListVector / LargeListVector for this layer</li>
   * </ol>
   *
   * @return the decoded ListVector or LargeListVector for this layer
   */
  public FieldVector decode() {
    FieldVector vector = field.createVector(allocator);
    if (vector instanceof ListVector) {
      ListVector listVec = (ListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      Field childField = field.getChildren().get(0);
      boolean childIsList = childField.getType() instanceof ArrowType.List
          || childField.getType() instanceof ArrowType.LargeList;
      FieldVector childVec;
      if (childIsList) {
        // Recurse first (bottom-up), matching Rust StructuralListDecodeTask::decode.
        // For constant layouts we estimate child rows; for rep/def layouts numRows
        // is ignored by the unraveler when levels are present.
        int childNumRows = estimateChildNumRows(unraveler.peekNextListLayer(), numRows);
        StructuralListDecodeTask childTask = new StructuralListDecodeTask(
            unraveler, innerVec, childField, childNumRows, allocator);
        childVec = childTask.decode();
      } else {
        childVec = innerVec;
      }

      // Now unravel the current list layer
      V21ListUnraveler.UnravelResult result = unraveler.unravelOffsets(numRows);

      // Write offsets
      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i < result.offsets.length; i++) {
        offsetBuf.setInt(i * 4, result.offsets[i]);
      }

      // Write validity
      for (int i = 0; i < numRows; i++) {
        if (i < result.validity.length && result.validity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
        }
      }

      // Attach child vector
      ValueVector dataVec = listVec.addOrGetVector(childField.getFieldType()).getVector();
      childVec.makeTransferPair(dataVec).transfer();
      if (childVec != innerVec) {
        childVec.close();
      }

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }

    if (vector instanceof LargeListVector) {
      LargeListVector listVec = (LargeListVector) vector;
      listVec.setInitialCapacity(numRows);
      listVec.allocateNew();

      Field childField = field.getChildren().get(0);
      boolean childIsList = childField.getType() instanceof ArrowType.List
          || childField.getType() instanceof ArrowType.LargeList;
      FieldVector childVec;
      if (childIsList) {
        int childNumRows = estimateChildNumRows(unraveler.peekNextListLayer(), numRows);
        StructuralListDecodeTask childTask = new StructuralListDecodeTask(
            unraveler, innerVec, childField, childNumRows, allocator);
        childVec = childTask.decode();
      } else {
        childVec = innerVec;
      }

      V21ListUnraveler.UnravelResult result = unraveler.unravelOffsets(numRows);

      // Write offsets (64-bit)
      org.apache.arrow.memory.ArrowBuf offsetBuf = listVec.getOffsetBuffer();
      for (int i = 0; i < result.offsets.length; i++) {
        offsetBuf.setLong(i * 8, result.offsets[i]);
      }

      // Write validity
      for (int i = 0; i < numRows; i++) {
        if (i < result.validity.length && result.validity[i]) {
          listVec.setNotNull(i);
        } else {
          listVec.setNull(i);
        }
      }

      // Attach child vector
      ValueVector dataVec = listVec.addOrGetVector(childField.getFieldType()).getVector();
      childVec.makeTransferPair(dataVec).transfer();
      if (childVec != innerVec) {
        childVec.close();
      }

      listVec.setLastSet(numRows - 1);
      listVec.setValueCount(numRows);
      return listVec;
    }

    throw new UnsupportedOperationException(
        "Unsupported list vector type: " + vector.getClass().getName());
  }

  /**
   * Estimates the number of child rows for a constant-layout list layer.
   *
   * <p>In a constant layout there are no rep/def levels, so the structure is inferred entirely
   * from the layer type. {@code ALL_VALID_LIST} defaults to one child per list; all other list
   * types default to zero children (empty or null).
   */
  private static int estimateChildNumRows(RepDefLayer layer, int numRows) {
    if (layer == RepDefLayer.REPDEF_ALL_VALID_LIST) {
      return numRows;
    }
    return 0;
  }
}
