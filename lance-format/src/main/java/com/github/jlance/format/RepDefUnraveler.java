// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

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
public class RepDefUnraveler {

  private short[] repLevels;
  private short[] defLevels;
  private final short[] originalRepLevels;
  private final short[] originalDefLevels;
  private final List<RepDefLayer> layers;
  private final int numItems;

  // Maps from definition level to the rep level at which that definition level is visible.
  // Mirrors Rust RepDefUnraveler.levels_to_rep.
  private final int[] levelsToRep;

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
  public RepDefUnraveler(short[] repLevels, short[] defLevels, List<RepDefLayer> layers) {
    this(repLevels, defLevels, layers, repLevels != null ? repLevels.length : 0);
  }

  public RepDefUnraveler(short[] repLevels, short[] defLevels, List<RepDefLayer> layers, int numItems) {
    this(repLevels, defLevels, layers, numItems, 0, 0, 0);
  }

  public RepDefUnraveler(short[] repLevels, short[] defLevels, List<RepDefLayer> layers,
      int numItems, int startLayer, int startDefCmp, int startRepCmp) {
    this.originalRepLevels = repLevels != null ? repLevels.clone() : new short[0];
    this.originalDefLevels = defLevels != null ? defLevels.clone() : new short[0];
    this.repLevels = repLevels != null ? repLevels.clone() : new short[0];
    this.defLevels = defLevels != null ? defLevels.clone() : new short[0];
    this.layers = layers;
    this.numItems = numItems;
    this.currentLayer = startLayer;
    this.currentDefCmp = startDefCmp;
    this.currentRepCmp = startRepCmp;
    this.levelsToRep = buildLevelsToRep(layers);
  }

  /**
   * Backward-compatible constructor that does not require an explicit {@code numItems}.
   */
  public RepDefUnraveler(short[] repLevels, short[] defLevels, List<RepDefLayer> layers,
      int startLayer, int startDefCmp, int startRepCmp) {
    this(repLevels, defLevels, layers, repLevels != null ? repLevels.length : 0,
        startLayer, startDefCmp, startRepCmp);
  }

  /**
   * Builds the levels_to_rep lookup table, mirroring Rust RepDefUnraveler::new().
   *
   * <p>Each entry maps a definition level value to the rep level at which that
   * definition level is visible.  Level 0 is always visible (maps to 0).
   */
  private static int[] buildLevelsToRep(List<RepDefLayer> layers) {
    int capacity = 1; // level 0 always exists
    for (RepDefLayer layer : layers) {
      capacity += defLevelsForLayer(layer);
    }
    int[] table = new int[capacity];
    int repCounter = 0;
    table[0] = 0;
    int idx = 1;
    for (RepDefLayer layer : layers) {
      switch (layer) {
        case REPDEF_ALL_VALID_ITEM:
        case REPDEF_ALL_VALID_LIST:
          break;
        case REPDEF_NULLABLE_ITEM:
          table[idx++] = repCounter;
          break;
        case REPDEF_NULLABLE_LIST:
        case REPDEF_EMPTYABLE_LIST:
          repCounter += 1;
          table[idx++] = repCounter;
          break;
        case REPDEF_NULL_AND_EMPTY_LIST:
          repCounter += 1;
          table[idx++] = repCounter;
          table[idx++] = repCounter;
          break;
        default:
          break;
      }
    }
    return table;
  }

  /**
   * Returns true if the current layer has no nulls (all valid).
   *
   * <p>Mirrors Rust {@code RepDefUnraveler::is_all_valid}.
   */
  public boolean isAllValid() {
    if (currentLayer >= layers.size()) {
      return true;
    }
    return defLevels == null || defLevels.length == 0
        || isAllValidLayer(layers.get(currentLayer));
  }

  private static boolean isAllValidLayer(RepDefLayer layer) {
    return layer == RepDefLayer.REPDEF_ALL_VALID_ITEM
        || layer == RepDefLayer.REPDEF_ALL_VALID_LIST
        || layer == RepDefLayer.REPDEF_EMPTYABLE_LIST;
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
   * Returns the maximum number of lists that could be present at the current layer.
   *
   * <p>Mirrors Rust {@code RepDefUnraveler::max_lists}.
   */
  public int maxLists() {
    if (repLevels != null && repLevels.length > 0) {
      return repLevels.length;
    }
    return 0;
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

    int repThreshold = currentRepCmp;
    currentRepCmp += 1;

    List<Integer> offsets = new ArrayList<>();
    List<Boolean> validityList = new ArrayList<>();
    int curlen = 0;

    if (hasDef) {
      int readIdx = 0;
      int writeIdx = 0;
      while (readIdx < repLevels.length) {
        short repVal = repLevels[readIdx];
        short defVal = defLevels[readIdx];

        if (repVal != 0) {
          // Same list element at this layer; keep for next layer
          repLevels[writeIdx] = (short) (repVal - 1);
          defLevels[writeIdx] = defVal;
          writeIdx++;

          if (defVal > maxLevel) {
            // Masked by upper null; invisible at this rep level.

          } else if (defVal == 0) {
            // Valid list
            offsets.add(curlen);
            curlen += 1;
            validityList.add(true);

          } else if (defVal == nullLevel || defVal > upperNull) {
            // Null list (or list masked by a null struct)
            offsets.add(curlen);
            validityList.add(false);

          } else if (defVal == emptyLevel) {
            // Empty list
            offsets.add(curlen);
            validityList.add(true);

          } else {
            // Valid list starting with null item
            offsets.add(curlen);
            curlen += 1;
            validityList.add(true);

          }
        } else {
          curlen += 1;

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
      // No def levels: every rep != 0 is kept, everything is valid
      int oldOffsetsLen = offsets.size();
      int writeIdx = 0;
      for (int i = 0; i < repLevels.length; i++) {
        short repVal = repLevels[i];
        if (repVal != 0) {
          offsets.add(curlen);
          repLevels[writeIdx] = (short) (repVal - 1);
          writeIdx++;
        }
        curlen += 1;
      }
      int numNewLists = offsets.size() - oldOffsetsLen;
      offsets.add(curlen);
      // Truncate repLevels
      if (writeIdx < repLevels.length) {
        short[] newRep = new short[writeIdx];
        System.arraycopy(repLevels, 0, newRep, 0, writeIdx);
        repLevels = newRep;
      }
      for (int i = 0; i < numNewLists; i++) {
        validityList.add(true);
      }
    }

    int[] offArray = new int[offsets.size()];
    for (int i = 0; i < offsets.size(); i++) {
      offArray[i] = offsets.get(i);
    }

    // For AllValidList and EmptyableList we don't need a validity bitmap.
    // (mirrors Rust where is_all_valid() is true for these layer types)
    boolean[] valArray = null;
    if (currentLayerType == RepDefLayer.REPDEF_NULLABLE_LIST
        || currentLayerType == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST) {
      valArray = new boolean[validityList.size()];
      for (int i = 0; i < validityList.size(); i++) {
        valArray[i] = validityList.get(i);
      }
    }

    return new UnravelResult(offArray, valArray);
  }

  /**
   * Handles ConstantLayout where rep/def buffers are empty.
   * All rows have the same structure determined entirely by the remaining layers.
   */
  private UnravelResult unravelConstantLayer(int numRows) {
    if (numRows < 0 || numRows > 1_000_000) {
      numRows = 0;
    }
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
        validity = null;
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = i;
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
        validity = null;
        for (int i = 0; i <= numRows; i++) {
          offsets[i] = 0;
        }
        break;
      case REPDEF_NULL_AND_EMPTY_LIST:
        // For constant layout with NULL_AND_EMPTY, we assume all null
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

  /**
   * Unravels a layer of validity from the definition levels.
   *
   * <p>Mirrors Rust {@code RepDefUnraveler::unravel_validity}.
   *
   * @param numValues number of values expected (used for AllValidItem shortcut)
   * @return a boolean array where {@code true} = valid, {@code false} = null;
   *         returns {@code null} if there are no nullable layers (all valid).
   */
  public boolean[] unravelValidity(int numValues) {
    if (currentLayer >= layers.size()) {
      boolean[] validity = new boolean[numValues];
      java.util.Arrays.fill(validity, true);
      return validity;
    }

    RepDefLayer meaning = layers.get(currentLayer);
    if (meaning == RepDefLayer.REPDEF_ALL_VALID_ITEM
        || defLevels == null || defLevels.length == 0) {
      currentLayer++;
      currentDefCmp += defLevelsForLayer(meaning);
      boolean[] validity = new boolean[numValues];
      java.util.Arrays.fill(validity, true);
      return validity;
    }

    currentLayer++;
    int currentDefCmpLocal = currentDefCmp;
    currentDefCmp += defLevelsForLayer(meaning);

    // Count filtered elements to allocate exact size
    int count = 0;
    for (int i = 0; i < defLevels.length; i++) {
      short level = defLevels[i];
      if (level >= 0 && level < levelsToRep.length && levelsToRep[level] <= currentRepCmp) {
        count++;
      }
    }

    boolean[] validity = new boolean[count];
    int idx = 0;
    for (int i = 0; i < defLevels.length; i++) {
      short level = defLevels[i];
      if (level >= 0 && level < levelsToRep.length && levelsToRep[level] <= currentRepCmp) {
        validity[idx++] = level <= currentDefCmpLocal;
      }
    }
    return validity;
  }

  /**
   * Convenience overload that uses the unraveler's {@code numItems} as the value count.
   */
  public boolean[] unravelValidity() {
    return unravelValidity(numItems);
  }

  public void skipValidity() {
    if (currentLayer < layers.size() && !isListLayer(layers.get(currentLayer))) {
      currentDefCmp += defLevelsForLayer(layers.get(currentLayer));
      currentLayer++;
    }
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
    }
  }

  public short[] getRepLevels() {
    return repLevels;
  }

  public short[] getDefLevels() {
    return defLevels;
  }

  public short[] getOriginalRepLevels() {
    return originalRepLevels;
  }

  public short[] getOriginalDefLevels() {
    return originalDefLevels;
  }

  public int getCurrentLayer() {
    return currentLayer;
  }

  public List<RepDefLayer> getLayers() {
    return layers;
  }

  public int getNumItems() {
    return numItems;
  }

  private static boolean isListLayer(RepDefLayer layer) {
    return layer == RepDefLayer.REPDEF_ALL_VALID_LIST
        || layer == RepDefLayer.REPDEF_NULLABLE_LIST
        || layer == RepDefLayer.REPDEF_EMPTYABLE_LIST
        || layer == RepDefLayer.REPDEF_NULL_AND_EMPTY_LIST;
  }
}
