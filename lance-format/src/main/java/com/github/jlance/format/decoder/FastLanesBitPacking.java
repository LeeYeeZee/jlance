// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

/**
 * Java implementation of the FastLanes bit-packing unpack algorithm.
 *
 * <p>This is a port of the Rust fastlanes crate used by Lance V2.1+ for
 * {@code InlineBitpacking} and {@code OutOfLineBitpacking} encodings.
 *
 * <p>FastLanes processes 1024 values per chunk, organized into SIMD lanes.
 * The {@code FL_ORDER} transpose pattern enables auto-vectorized decoding.
 *
 * <p>Supported uncompressed types: u8, u16, u32, u64 (corresponding to
 * 8, 16, 32, 64 bits per value).
 */
public final class FastLanesBitPacking {

  private FastLanesBitPacking() {}

  /** FastLanes transpose order for 8 rows. */
  private static final int[] FL_ORDER = {0, 4, 2, 6, 1, 5, 3, 7};

  /** Returns a mask with the low {@code bits} bits set. */
  private static long mask(int bits) {
    if (bits >= 64) {
      return ~0L;
    }
    return (1L << bits) - 1;
  }

  /**
   * Unpacks 1024 u8 values from FastLanes bit-packed format.
   *
   * @param bitWidth number of bits per packed value (1..8)
   * @param packed source words (each word is a {@code byte} stored in the low 8 bits of each int)
   * @param packedOffset offset in {@code packed}
   * @param output destination array (must have room for 1024 values at {@code outputOffset})
   * @param outputOffset offset in {@code output}
   */
  public static void unpackU8(int bitWidth, int[] packed, int packedOffset, byte[] output, int outputOffset) {
    if (bitWidth == 0) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = 0;
      }
      return;
    }
    if (bitWidth == 8) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = (byte) (packed[packedOffset + i] & 0xFF);
      }
      return;
    }
    final int T = 8;
    final int LANES = 1024 / T; // 128

    for (int lane = 0; lane < LANES; lane++) {
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int srcIdx = packedOffset + LANES * currWord + lane;
        long src = packed[srcIdx] & 0xFFL;

        long tmp;
        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          tmp = (src >>> shift) & mask(currentBits);
          if (remainingBits > 0) {
            int nextIdx = packedOffset + LANES * nextWord + lane;
            long nextSrc = packed[nextIdx] & 0xFFL;
            tmp |= (nextSrc & mask(remainingBits)) << currentBits;
          }
        } else {
          tmp = (src >>> shift) & mask(bitWidth);
        }

        int o = row / 8;
        int s = row % 8;
        int idx = FL_ORDER[o] * 16 + s * 128 + lane;
        output[outputOffset + idx] = (byte) tmp;
      }
    }
  }

  /**
   * Unpacks 1024 u16 values from FastLanes bit-packed format.
   *
   * @param bitWidth number of bits per packed value (1..16)
   * @param packed source words (each word is a {@code short} stored in the low 16 bits of each int)
   * @param packedOffset offset in {@code packed}
   * @param output destination array (must have room for 1024 values at {@code outputOffset})
   * @param outputOffset offset in {@code output}
   */
  public static void unpackU16(int bitWidth, int[] packed, int packedOffset, short[] output, int outputOffset) {
    if (bitWidth == 0) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = 0;
      }
      return;
    }
    if (bitWidth == 16) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = (short) (packed[packedOffset + i] & 0xFFFF);
      }
      return;
    }
    final int T = 16;
    final int LANES = 1024 / T; // 64

    for (int lane = 0; lane < LANES; lane++) {
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int srcIdx = packedOffset + LANES * currWord + lane;
        long src = packed[srcIdx] & 0xFFFFL;

        long tmp;
        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          tmp = (src >>> shift) & mask(currentBits);
          if (remainingBits > 0) {
            int nextIdx = packedOffset + LANES * nextWord + lane;
            long nextSrc = packed[nextIdx] & 0xFFFFL;
            tmp |= (nextSrc & mask(remainingBits)) << currentBits;
          }
        } else {
          tmp = (src >>> shift) & mask(bitWidth);
        }

        int o = row / 8;
        int s = row % 8;
        int idx = FL_ORDER[o] * 16 + s * 128 + lane;
        output[outputOffset + idx] = (short) tmp;
      }
    }
  }

  /**
   * Unpacks 1024 u32 values from FastLanes bit-packed format.
   *
   * @param bitWidth number of bits per packed value (1..32)
   * @param packed source words (each word is an {@code int})
   * @param packedOffset offset in {@code packed}
   * @param output destination array (must have room for 1024 values at {@code outputOffset})
   * @param outputOffset offset in {@code output}
   */
  public static void unpackU32(int bitWidth, int[] packed, int packedOffset, int[] output, int outputOffset) {
    if (bitWidth == 0) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = 0;
      }
      return;
    }
    if (bitWidth == 32) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = packed[packedOffset + i];
      }
      return;
    }
    final int T = 32;
    final int LANES = 1024 / T; // 32

    for (int lane = 0; lane < LANES; lane++) {
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int srcIdx = packedOffset + LANES * currWord + lane;
        long src = packed[srcIdx] & 0xFFFFFFFFL;

        long tmp;
        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          tmp = (src >>> shift) & mask(currentBits);
          if (remainingBits > 0) {
            int nextIdx = packedOffset + LANES * nextWord + lane;
            long nextSrc = packed[nextIdx] & 0xFFFFFFFFL;
            tmp |= (nextSrc & mask(remainingBits)) << currentBits;
          }
        } else {
          tmp = (src >>> shift) & mask(bitWidth);
        }

        int o = row / 8;
        int s = row % 8;
        int idx = FL_ORDER[o] * 16 + s * 128 + lane;
        output[outputOffset + idx] = (int) tmp;
      }
    }
  }

  /**
   * Packs 1024 u32 values into FastLanes bit-packed format.
   *
   * <p>This is the inverse of {@link #unpackU32}. The {@code packed} array must be
   * zero-initialized by the caller.
   *
   * @param bitWidth number of bits per packed value (1..32)
   * @param input source values (1024 values at {@code inputOffset})
   * @param inputOffset offset in {@code input}
   * @param packed destination words (each word is an {@code int}), must be zero-initialized
   * @param packedOffset offset in {@code packed}
   */
  public static void packU32(int bitWidth, int[] input, int inputOffset, int[] packed, int packedOffset) {
    if (bitWidth == 0 || bitWidth == 32) {
      for (int i = 0; i < 1024; i++) {
        packed[packedOffset + i] = input[inputOffset + i];
      }
      return;
    }
    final int T = 32;
    final int LANES = 1024 / T; // 32

    for (int lane = 0; lane < LANES; lane++) {
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int srcIdx = packedOffset + LANES * currWord + lane;

        int o = row / 8;
        int s = row % 8;
        int idx = FL_ORDER[o] * 16 + s * 128 + lane;
        int value = (int) (input[inputOffset + idx] & mask(bitWidth));

        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          packed[srcIdx] |= (value & (int) mask(currentBits)) << shift;
          if (remainingBits > 0) {
            int nextIdx = packedOffset + LANES * nextWord + lane;
            packed[nextIdx] |= (value >>> currentBits) & (int) mask(remainingBits);
          }
        } else {
          packed[srcIdx] |= (value & (int) mask(bitWidth)) << shift;
        }
      }
    }
  }

  /**
   * Unpacks 1024 u64 values from FastLanes bit-packed format.
   *
   * @param bitWidth number of bits per packed value (1..64)
   * @param packed source words (each word is a {@code long})
   * @param packedOffset offset in {@code packed}
   * @param output destination array (must have room for 1024 values at {@code outputOffset})
   * @param outputOffset offset in {@code output}
   */
  public static void unpackU64(int bitWidth, long[] packed, int packedOffset, long[] output, int outputOffset) {
    if (bitWidth == 0) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = 0;
      }
      return;
    }
    if (bitWidth == 64) {
      for (int i = 0; i < 1024; i++) {
        output[outputOffset + i] = packed[packedOffset + i];
      }
      return;
    }
    final int T = 64;
    final int LANES = 1024 / T; // 16

    for (int lane = 0; lane < LANES; lane++) {
      for (int row = 0; row < T; row++) {
        int currWord = (row * bitWidth) / T;
        int nextWord = ((row + 1) * bitWidth) / T;
        int shift = (row * bitWidth) % T;
        int srcIdx = packedOffset + LANES * currWord + lane;
        long src = packed[srcIdx];

        long tmp;
        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % T;
          int currentBits = bitWidth - remainingBits;
          tmp = (src >>> shift) & mask(currentBits);
          if (remainingBits > 0) {
            int nextIdx = packedOffset + LANES * nextWord + lane;
            long nextSrc = packed[nextIdx];
            tmp |= (nextSrc & mask(remainingBits)) << currentBits;
          }
        } else {
          tmp = (src >>> shift) & mask(bitWidth);
        }

        int o = row / 8;
        int s = row % 8;
        int idx = FL_ORDER[o] * 16 + s * 128 + lane;
        output[outputOffset + idx] = tmp;
      }
    }
  }
}
