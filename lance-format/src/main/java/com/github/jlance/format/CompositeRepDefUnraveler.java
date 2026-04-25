// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format;

import java.util.ArrayList;
import java.util.List;

/**
 * Composite unraveler that seamlessly merges multiple {@link RepDefUnraveler}s.
 *
 * <p>Mirrors Rust {@code CompositeRepDefUnraveler} from
 * {@code lance-encoding/src/repdef.rs}.  Each inner unraveler may come from a
 * different page or mini-block and can have a completely different layer
 * interpretation (e.g. one page might contain null items but no null structs
 * and the next page might have null structs but no null items).
 */
public class CompositeRepDefUnraveler {

  private final List<RepDefUnraveler> unravelers;

  public CompositeRepDefUnraveler(List<RepDefUnraveler> unravelers) {
    this.unravelers = unravelers;
  }

  /**
   * Unravels a layer of validity.
   *
   * @param numValues total number of values expected across all unravelers
   * @return {@code null} if every inner unraveler reports all-valid;
   *         otherwise a boolean array where {@code true} = valid,
   *         {@code false} = null.
   */
  public boolean[] unravelValidity(int numValues) {
    boolean isAllValid = true;
    for (RepDefUnraveler u : unravelers) {
      isAllValid &= u.isAllValid();
    }

    if (isAllValid) {
      for (RepDefUnraveler u : unravelers) {
        u.skipValidity();
      }
      return null;
    }

    // Each unraveler appends its own portion.  We concatenate them into a
    // single array whose total length should equal numValues.
    List<boolean[]> parts = new ArrayList<>();
    int totalLen = 0;
    for (RepDefUnraveler u : unravelers) {
      boolean[] part = u.unravelValidity(u.getNumItems());
      if (part != null) {
        parts.add(part);
        totalLen += part.length;
      }
    }

    if (totalLen == 0 || numValues == 0) {
      return null;
    }

    boolean[] validity = new boolean[totalLen];
    int offset = 0;
    for (boolean[] part : parts) {
      System.arraycopy(part, 0, validity, offset, part.length);
      offset += part.length;
    }
    return validity;
  }

  /**
   * Unravels a layer of offsets (and the validity for that layer).
   *
   * @return merged offsets and optional validity across all inner unravelers
   */
  public RepDefUnraveler.UnravelResult unravelOffsets() {
    boolean isAllValid = true;
    int maxNumLists = 0;
    for (RepDefUnraveler u : unravelers) {

      isAllValid &= u.isAllValid();
      maxNumLists += u.maxLists();
    }

    List<Integer> offsets = new ArrayList<>();
    List<Boolean> validityList = new ArrayList<>();

    for (RepDefUnraveler u : unravelers) {
      int numRows = u.maxLists() > 0 ? u.maxLists() : u.getNumItems();
      RepDefUnraveler.UnravelResult part = u.unravelOffsets(numRows);
      // Merge offsets: if this is not the first unraveler, pop the trailing
      // offset (which is the length of the previous segment) and continue.
      int base = offsets.isEmpty() ? 0 : offsets.get(offsets.size() - 1);
      if (!offsets.isEmpty() && part.offsets.length > 0) {
        offsets.remove(offsets.size() - 1);
      }
      for (int off : part.offsets) {
        offsets.add(off + base);
      }
      if (part.validity != null) {
        for (boolean v : part.validity) {
          validityList.add(v);
        }
      } else {
        // All-valid layer: append true for each list
        for (int i = 0; i < part.numLists; i++) {
          validityList.add(true);
        }
      }
    }

    int[] offArray = new int[offsets.size()];
    for (int i = 0; i < offsets.size(); i++) {
      offArray[i] = offsets.get(i);
    }


    boolean[] valArray = null;
    if (!isAllValid && !validityList.isEmpty()) {
      valArray = new boolean[validityList.size()];
      for (int i = 0; i < validityList.size(); i++) {
        valArray[i] = validityList.get(i);
      }
    }

    return new RepDefUnraveler.UnravelResult(offArray, valArray);
  }
}
