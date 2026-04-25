// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import com.github.jlance.format.LanceFileFooter;
import com.github.jlance.format.RepDefUnraveler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lance.encodings21.EncodingsV21.RepDefLayer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Decodes a V2.1+ struct column by decoding all children independently
 * and deriving struct validity from their shared rep/def state.
 *
 * <p>This is the Java analog of the Rust {@code StructuralStructDecodeTask}
 * from {@code lance-encoding/src/encodings/logical/struct.rs}.
 *
 * <p>The task mirrors the Rust design:
 * <ol>
 *   <li>Share parent rep/def with all children</li>
 *   <li>Decode each child independently with the same starting state</li>
 *   <li>Compute struct value count from the first child's actual value count</li>
 *   <li>Allocate {@link StructVector} and transfer decoded children</li>
 *   <li>Derive struct validity from the children's rep/def</li>
 * </ol>
 */
public class StructuralStructDecodeTask {

  private final Field field;
  private final int numRows;
  private final BufferAllocator allocator;
  private final LanceFileFooter footer;
  private final int[] nextColIndex;

  /**
   * Functional interface for decoding a child field with a given rep/def state.
   */
  @FunctionalInterface
  public interface ChildFieldDecoder {
    /**
     * Decodes a single child field.
     *
     * @param childField   the Arrow field to decode
     * @param parentRep    parent repetition levels (may be {@code null})
     * @param parentDef    parent definition levels (may be {@code null})
     * @param parentLayers parent rep/def layers (may be {@code null})
     * @return the decoded array for this child
     * @throws IOException if decoding fails
     */
    DecodedArray decode(
        Field childField,
        short[] parentRep,
        short[] parentDef,
        List<RepDefLayer> parentLayers) throws IOException;
  }

  public StructuralStructDecodeTask(
      Field field,
      int numRows,
      BufferAllocator allocator,
      LanceFileFooter footer,
      int[] nextColIndex) {
    this.field = field;
    this.numRows = numRows;
    this.allocator = allocator;
    this.footer = footer;
    this.nextColIndex = nextColIndex;
  }

  /**
   * Decodes the struct and its children.
   *
   * @param parentRepLevels   parent repetition levels
   * @param parentDefLevels   parent definition levels
   * @param parentLayers      parent rep/def layers
   * @param childDecoder      callback to decode each child field
   * @return decoded struct array with rep/def state propagated from the first child
   * @throws IOException if decoding fails
   */
  public DecodedArray decode(
      short[] parentRepLevels,
      short[] parentDefLevels,
      List<RepDefLayer> parentLayers,
      ChildFieldDecoder childDecoder) throws IOException {

    boolean isV21 = footer.isV2_1OrLater();
    boolean isNullableStruct = isV21 && field.isNullable();
    List<DecodedArray> childArrays = new ArrayList<>();
    List<Integer> childValueCounts = new ArrayList<>();

    // Decode all children first so we know the actual value count.
    for (int ci = 0; ci < field.getChildren().size(); ci++) {
      Field childField = field.getChildren().get(ci);
      DecodedArray childArray = childDecoder.decode(
          childField, parentRepLevels, parentDefLevels, parentLayers);
      childArrays.add(childArray);
      childValueCounts.add(childArray.vector.getValueCount());
      // If the child consumed rep/def (e.g. a list), subsequent children should
      // receive the original un-truncated rep/def.  Re-clone for each child.
      if (parentRepLevels != null && childArray.repLevels != null
          && childArray.repLevels != parentRepLevels) {
        parentRepLevels = parentRepLevels.clone();
        if (parentDefLevels != null) {
          parentDefLevels = parentDefLevels.clone();
        }
      }
    }

    int structValueCount = numRows;
    if (!childValueCounts.isEmpty()) {
      structValueCount = childValueCounts.get(0);
    }

    // Allocate struct with the correct capacity.  We do this AFTER children are
    // decoded so that we know the real value count (e.g. a struct inside a list
    // may have more values than numRows).
    StructVector struct = (StructVector) field.createVector(allocator);
    struct.setInitialCapacity(structValueCount);
    struct.allocateNew();
    for (int i = 0; i < structValueCount; i++) {
      BitVectorHelper.setValidityBit(struct.getValidityBuffer(), i, 1);
    }

    // Transfer decoded children into the struct.
    for (int ci = 0; ci < field.getChildren().size(); ci++) {
      Field childField = field.getChildren().get(ci);
      FieldVector childVec = childArrays.get(ci).vector;
      @SuppressWarnings("unchecked")
      FieldVector slot =
          struct.addOrGet(
              childField.getName(), childField.getFieldType(),
              (Class<? extends FieldVector>) childVec.getClass());
      childVec.makeTransferPair(slot).transfer();
      childVec.close();
    }

    // V2.1+ nullable struct: derive validity from the shared rep/def unraveler.
    // The first child's unraveler has already had its inner layers consumed by the
    // page decoder (via skipValidity); the next layer is the struct's NullableItem.
    // This mirrors Rust RepDefStructDecodeTask::decode where repdef.unravel_validity()
    // is called after all children have consumed their layers.
    if (isNullableStruct && !childArrays.isEmpty()) {
      RepDefUnraveler unraveler = childArrays.get(0).unraveler;
      if (unraveler != null) {
        boolean[] validity = unraveler.unravelValidity(structValueCount);
        if (validity != null) {
          for (int i = 0; i < validity.length && i < structValueCount; i++) {
            BitVectorHelper.setValidityBit(
                struct.getValidityBuffer(), i, validity[i] ? 1 : 0);
          }
        }
      }
    }

    struct.setValueCount(structValueCount);
    // Propagate the first child's unraveler upward so that an enclosing list layer
    // can continue consuming rep/def from the correct state.
    if (!childArrays.isEmpty() && childArrays.get(0).unraveler != null) {
      return new DecodedArray(struct, childArrays.get(0).unraveler);
    }
    return new DecodedArray(struct);
  }

}
