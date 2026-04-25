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

    // V2.1+ nullable struct: derive validity from first child's ORIGINAL rep/def levels.
    // We must use the pre-truncated levels because struct nullability is orthogonal to
    // any list layers that the child may have consumed.
    if (isNullableStruct && !childArrays.isEmpty()) {
      DecodedArray firstChild = childArrays.get(0);
      RepDefUnraveler childUnraveler = firstChild.unraveler;
      short[] def = childUnraveler != null
          ? childUnraveler.getOriginalDefLevels() : firstChild.defLevels;
      short[] rep = childUnraveler != null
          ? childUnraveler.getOriginalRepLevels() : firstChild.repLevels;
      List<RepDefLayer> layers = childUnraveler != null
          ? childUnraveler.getLayers() : firstChild.layers;
      if (def != null && def.length > 0 && layers != null) {
        int nullStructLevel = computeOuterNullItemLevel(layers);
        if (nullStructLevel >= 0) {
          if (rep != null && rep.length > 0) {
            // List child: rep > 0 marks a new row.  Check the first entry of each row.
            int entryIdx = 0;
            for (int row = 0; row < structValueCount && entryIdx < rep.length; row++) {
              boolean structNull = (def[entryIdx] == nullStructLevel);
              BitVectorHelper.setValidityBit(
                  struct.getValidityBuffer(), row, structNull ? 0 : 1);
              // Advance to the start of the next row.
              entryIdx++;
              while (entryIdx < rep.length && rep[entryIdx] == 0) {
                entryIdx++;
              }
            }
          } else {
            // Primitive child (no list layer)
            for (int i = 0; i < structValueCount && i < def.length; i++) {
              boolean structNull = (def[i] == nullStructLevel);
              BitVectorHelper.setValidityBit(
                  struct.getValidityBuffer(), i, structNull ? 0 : 1);
            }
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

  /**
   * Computes the definition level at which a struct item is considered null.
   *
   * <p>Scans layers inner-to-outer and returns the level corresponding to the
   * first {@code NullableItem} layer encountered.  Returns -1 if no nullable
   * item layer exists.
   */
  private static int computeOuterNullItemLevel(List<RepDefLayer> layers) {
    int currentDef = 0;
    int outerNullItemLevel = -1;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          outerNullItemLevel = currentDef + 1;
          currentDef += 1;
          break;
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          currentDef += 1;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          currentDef += 2;
          break;
        default:
          break;
      }
    }
    return outerNullItemLevel;
  }
}
