package com.github.jlance.format;

import java.util.ArrayList;
import java.util.List;
import lance.encodings21.EncodingsV21.RepDefLayer;

/**
 * Unravels repetition and definition levels into Arrow list offsets and validity bitmaps.
 *
 * <p>This is a Java translation of the Rust {@code RepDefUnraveler} from the canonical
 * {@code lance-encoding} crate (see {@code lance-encoding/src/repdef.rs}).
 *
 * <p>The unraveler processes one list layer per call to {@link #unravelOffsets(int)}.
 * After each call, non-zero rep levels are decremented by 1 and the buffers are truncated
 * so the next layer sees the correct boundaries. This naturally supports arbitrary nesting
 * depth ({@code list<list<list<...>>>}).
 *
 * <p>Layers are ordered <strong>inner-to-outer</strong> (same as the protobuf {@code layers}
 * field in {@code MiniBlockLayout} / {@code ConstantLayout}).
 */
public class V21ListUnraveler {

  private short[] repLevels;
  private short[] defLevels;
  private final List<RepDefLayer> layers;

  private int currentLayer;
  private int currentDefCmp;
  private int currentRepCmp;

  /**
   * Result of unravelling one list layer.
   */
  public static class UnravelResult {
    public final int[] offsets;
    public final boolean[] validity;
    public final int numLists;

    UnravelResult(int[] offsets, boolean[] validity) {
      this.offsets = offsets;
      this.validity = validity;
      this.numLists = offsets.length - 1;
    }
  }

  /**
   * Creates a new unraveler.
   *
   * @param repLevels repetition levels (may be empty for ConstantLayout with zero values)
   * @param defLevels definition levels (may be empty)
   * @param layers    layer definitions, inner-to-outer
   */
  public V21ListUnraveler(short[] repLevels, short[] defLevels, List<RepDefLayer> layers) {
    this.repLevels = repLevels != null ? repLevels : new short[0];
    this.defLevels = defLevels != null ? defLevels : new short[0];
    this.layers = layers;
    this.currentLayer = 0;
    this.currentDefCmp = 0;
    this.currentRepCmp = 0;
  }

  /**
   * Returns the type of the next list layer (without consuming it), or null if none.
   */
  public RepDefLayer peekNextListLayer() {
    for (int i = currentLayer; i < layers.size(); i++) {
      if (isListLayer(layers.get(i))) {
        return layers.get(i);
      }
    }
    return null;
  }

  /**
   * Returns true if there are still list layers that have not been unravelled.
   */
  public boolean hasMoreListLayers() {
    for (int i = currentLayer; i < layers.size(); i++) {
      if (isListLayer(layers.get(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the number of list layers in the layer stack.
   */
  public int listLayerCount() {
    int count = 0;
    for (RepDefLayer layer : layers) {
      if (isListLayer(layer)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Unravels the next list layer into offsets and a validity bitmap.
   *
   * @param numRows number of rows (lists) expected at this layer
   * @return offsets + validity for the current layer
   */
  public UnravelResult unravelOffsets(int numRows) {
    if (repLevels.length == 0 && defLevels.length == 0) {
      return unravelConstantLayer(numRows);
    }

    // Skip non-list layers (e.g. AllValidItem, NullableItem) at the current position.
    // This aligns with Rust's RepDefUnraveler where unravel_offsets is only called
    // for list layers, and item layers are handled by the primitive decoder.
    while (currentLayer < layers.size() && !isListLayer(layers.get(currentLayer))) {
      currentDefCmp += defLevelsForLayer(layers.get(currentLayer));
      currentLayer++;
    }

    boolean hasDef = defLevels.length > 0;

    // Determine null_level and empty_level for the current layer
    int validLevel = currentDefCmp;
    int nullLevel;
    int emptyLevel;

    RepDefLayer currentLayerType = layers.get(currentLayer);
    switch (currentLayerType) {
      case REPDEF_NULLABLE_LIST:
        nullLevel = validLevel + 1;
        emptyLevel = 0;
        currentDefCmp += 1;
        break;
      case REPDEF_EMPTYABLE_LIST:
        nullLevel = 0;
        emptyLevel = validLevel + 1;
        currentDefCmp += 1;
        break;
      case REPDEF_NULL_AND_EMPTY_LIST:
        nullLevel = validLevel + 1;
        emptyLevel = validLevel + 2;
        currentDefCmp += 2;
        break;
      case REPDEF_ALL_VALID_LIST:
        nullLevel = 0;
        emptyLevel = 0;
        break;
      default:
        throw new IllegalStateException(
            "Expected a list layer but got " + currentLayerType + " at layer " + currentLayer);
    }
    currentLayer++;

    // Compute max_level: the highest def that is still visible at this rep level.
    // Account for inner NullableItem layers that may appear before the next list layer.
    int maxLevel = Math.max(nullLevel, Math.max(emptyLevel, validLevel));
    int upperNull = maxLevel;
    for (int i = currentLayer; i < layers.size(); i++) {
      RepDefLayer layer = layers.get(i);
      if (layer == RepDefLayer.REPDEF_NULLABLE_ITEM) {
        maxLevel += 1;
      } else if (layer == RepDefLayer.REPDEF_ALL_VALID_ITEM) {
        // no change
      } else {
        break; // hit another list layer or struct layer
      }
    }

    currentRepCmp += 1;

    List<Integer> offsets = new ArrayList<>();
    List<Boolean> validityList = new ArrayList<>();
    int curlen = 0;

    // If offsets already has a trailing value from a previous page/unravel, pop it
    // (not needed in our merged-buffer case, but kept for parity with Rust)

    if (hasDef) {
      int readIdx = 0;
      int writeIdx = 0;
      // Debug removed
      while (readIdx < repLevels.length) {
        short repVal = repLevels[readIdx];
        short defVal = defLevels[readIdx];
        // Debug removed
        if (repVal != 0) {
          // Compact: shift rep down by 1 so the next layer sees correct boundaries
          repLevels[writeIdx] = (short) (repVal - 1);
          defLevels[writeIdx] = defVal;
          writeIdx++;

          if (defVal == 0) {
            // Valid list
            offsets.add(curlen);
            curlen += 1;
            validityList.add(true);
            // Debug removed
          } else if (defVal > maxLevel) {
            // Masked by upper null; invisible at this rep level.
            // Do NOT add offset, but keep in buffers for upper layers.
            // Debug removed
          } else if (defVal == nullLevel || defVal > upperNull) {
            // Null list (or list masked by a null struct)
            offsets.add(curlen);
            validityList.add(false);
            // Debug removed
          } else if (defVal == emptyLevel) {
            // Empty list
            offsets.add(curlen);
            validityList.add(true);
            // Debug removed
          } else {
            // New valid list starting with null item
            offsets.add(curlen);
            curlen += 1;
            validityList.add(true);
            // Debug removed
          }
        } else {
          curlen += 1;
          // Debug removed
        }
        readIdx++;
      }
      // Append final offset
      offsets.add(curlen);
      // Truncate buffers to compacted length
      if (writeIdx < repLevels.length) {
        short[] newRep = new short[writeIdx];
        short[] newDef = new short[writeIdx];
        System.arraycopy(repLevels, 0, newRep, 0, writeIdx);
        System.arraycopy(defLevels, 0, newDef, 0, writeIdx);
        repLevels = newRep;
        defLevels = newDef;
      }
    } else {
      // No def levels: every rep != 0 is a new list, everything is valid
      int oldOffsetsLen = offsets.size();
      for (int i = 0; i < repLevels.length; i++) {
        short repVal = repLevels[i];
        if (repVal != 0) {
          offsets.add(curlen);
          repLevels[i] = (short) (repVal - 1);
        }
        curlen += 1;
      }
      int numNewLists = offsets.size() - oldOffsetsLen;
      offsets.add(curlen);
      // Truncate repLevels (all entries were kept, just shifted)
      if (repLevels.length > 0) {
        // In the no-def case, every entry is kept (shifted down by 1)
        // Nothing to truncate
      }
      for (int i = 0; i < numNewLists; i++) {
        validityList.add(true);
      }
    }

    int[] offArray = new int[offsets.size()];
    for (int i = 0; i < offsets.size(); i++) {
      offArray[i] = offsets.get(i);
    }
    boolean[] valArray = new boolean[validityList.size()];
    for (int i = 0; i < validityList.size(); i++) {
      valArray[i] = validityList.get(i);
    }

    return new UnravelResult(offArray, valArray);
  }

  /**
   * Handles ConstantLayout where rep/def buffers are empty.
   * All rows have the same structure determined entirely by the remaining layers.
   */
  private UnravelResult unravelConstantLayer(int numRows) {
    // Skip non-list layers (same as unravelOffsets)
    while (currentLayer < layers.size() && !isListLayer(layers.get(currentLayer))) {
      currentDefCmp += defLevelsForLayer(layers.get(currentLayer));
      currentLayer++;
    }
    if (currentLayer >= layers.size()) {
      throw new IllegalStateException("No more list layers available. currentLayer=" + currentLayer + ", layers=" + layers);
    }
    RepDefLayer currentLayerType = layers.get(currentLayer);
    currentLayer++;
    currentDefCmp += defLevelsForLayer(currentLayerType);
    currentRepCmp += 1;

    int[] offsets;
    boolean[] validity;

    switch (currentLayerType) {
      case REPDEF_ALL_VALID_LIST:
        offsets = new int[numRows + 1];
        validity = new boolean[numRows];
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = i;
        }
        for (int i = 0; i < numRows; i++) {
          validity[i] = true;
        }
        break;
      case REPDEF_NULLABLE_LIST:
        offsets = new int[numRows + 1];
        validity = new boolean[numRows];
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = 0;
        }
        for (int i = 0; i < numRows; i++) {
          validity[i] = false;
        }
        break;
      case REPDEF_EMPTYABLE_LIST:
        offsets = new int[numRows + 1];
        validity = new boolean[numRows];
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = 0;
        }
        for (int i = 0; i < numRows; i++) {
          validity[i] = true;
        }
        break;
      case REPDEF_NULL_AND_EMPTY_LIST:
        // For constant layout with NULL_AND_EMPTY, we assume all null (consistent with M42)
        offsets = new int[numRows + 1];
        validity = new boolean[numRows];
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = 0;
        }
        for (int i = 0; i < numRows; i++) {
          validity[i] = false;
        }
        break;
      default:
        throw new IllegalStateException(
            "Expected a list layer but got " + currentLayerType + " at layer " + (currentLayer - 1));
    }

    return new UnravelResult(offsets, validity);
  }

  private static int defLevelsForLayer(RepDefLayer layer) {
    switch (layer) {
      case REPDEF_ALL_VALID_ITEM:
      case REPDEF_ALL_VALID_LIST:
        return 0;
      case REPDEF_NULLABLE_ITEM:
      case REPDEF_NULLABLE_LIST:
      case REPDEF_EMPTYABLE_LIST:
        return 1;
      case REPDEF_NULL_AND_EMPTY_LIST:
        return 2;
      default:
        return 0;
    }
  }

  /**
   * Skips the given number of list layers without computing offsets.
   * Updates internal state and truncates rep/def buffers just as
   * {@link #unravelOffsets(int)} would, so subsequent calls continue
   * from the next outer list layer.
   */
  public void skipListLayers(int count) {
    for (int i = 0; i < count; i++) {
      while (currentLayer < layers.size() && !isListLayer(layers.get(currentLayer))) {
        currentDefCmp += defLevelsForLayer(layers.get(currentLayer));
        currentLayer++;
      }
      if (currentLayer >= layers.size()) {
        break;
      }
      RepDefLayer currentLayerType = layers.get(currentLayer);
      currentLayer++;
      currentDefCmp += defLevelsForLayer(currentLayerType);
      currentRepCmp += 1;

      // Truncate rep/def exactly as unravelOffsets does for rep != 0 entries
      if (repLevels.length > 0) {
        int writeIdx = 0;
        for (int j = 0; j < repLevels.length; j++) {
          if (repLevels[j] != 0) {
            repLevels[writeIdx] = (short) (repLevels[j] - 1);
            if (defLevels.length > 0) {
              defLevels[writeIdx] = defLevels[j];
            }
            writeIdx++;
          }
        }
        if (writeIdx < repLevels.length) {
          short[] newRep = new short[writeIdx];
          short[] newDef = new short[writeIdx];
          System.arraycopy(repLevels, 0, newRep, 0, writeIdx);
          System.arraycopy(defLevels, 0, newDef, 0, writeIdx);
          repLevels = newRep;
          defLevels = newDef;
        }
      }
    }
  }

  public short[] getRepLevels() {
    return repLevels;
  }

  public short[] getDefLevels() {
    return defLevels;
  }

  public int getCurrentLayer() {
    return currentLayer;
  }

  private static boolean isListLayer(RepDefLayer layer) {
    return layer == RepDefLayer.REPDEF_ALL_VALID_LIST
        || layer == RepDefLayer.REPDEF_NULLABLE_LIST
        || layer == RepDefLayer.REPDEF_EMPTYABLE_LIST
        || layer == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST;
  }
}
