package com.github.jlance.compat;

import static org.junit.jupiter.api.Assertions.*;

import com.github.jlance.format.decoder.FastLanesBitPacking;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for FastLanes bit-packing pack/unpack.
 *
 * <p>These tests verify that our Java unpack implementation is correct by
 * pairing it with a Java pack implementation and checking round-trip equality.
 */
public class FastLanesRoundTripTest {

  private static final int[] FL_ORDER = {0, 4, 2, 6, 1, 5, 3, 7};

  /** Pack 1024 u32 values into FastLanes format. */
  private static int[] packU32(int bitWidth, int[] values) {
    if (bitWidth == 32) {
      return values.clone();
    }
    int numPackedInts = (bitWidth * 1024 + 31) / 32;
    int[] packed = new int[numPackedInts];
    final int LANES = 32;
    for (int row = 0; row < 32; row++) {
      for (int lane = 0; lane < LANES; lane++) {
        int idx = FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane;
        long value = values[idx] & ((1L << bitWidth) - 1L);
        int currWord = (row * bitWidth) / 32;
        int shift = (row * bitWidth) % 32;
        int intIdx = LANES * currWord + lane;
        int nextWord = ((row + 1) * bitWidth) / 32;
        if (nextWord > currWord) {
          int remainingBits = ((row + 1) * bitWidth) % 32;
          int currentBits = bitWidth - remainingBits;
          long mask = (1L << currentBits) - 1L;
          packed[intIdx] |= (int) ((value & mask) << shift);
          if (remainingBits > 0) {
            int nextIdx = LANES * nextWord + lane;
            long remMask = (1L << remainingBits) - 1L;
            packed[nextIdx] |= (int) ((value >> currentBits) & remMask);
          }
        } else {
          long mask = (1L << bitWidth) - 1L;
          packed[intIdx] |= (int) ((value & mask) << shift);
        }
      }
    }
    return packed;
  }

  @Test
  public void testRoundTripU32BitWidth4() {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i % 16; // 0..15, fits in 4 bits
    }
    // Transpose values into FastLanes order
    int[] transposed = new int[1024];
    for (int row = 0; row < 32; row++) {
      for (int lane = 0; lane < 32; lane++) {
        int idx = FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane;
        transposed[idx] = values[row * 32 + lane];
      }
    }

    int[] packed = packU32(4, transposed);
    int[] unpacked = new int[1024];
    FastLanesBitPacking.unpackU32(4, packed, 0, unpacked, 0);

    // Unpack returns values in transposed order, convert back to row-major
    int[] result = new int[1024];
    for (int row = 0; row < 32; row++) {
      for (int lane = 0; lane < 32; lane++) {
        int idx = FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane;
        result[row * 32 + lane] = unpacked[idx];
      }
    }

    assertArrayEquals(values, result, "Round-trip mismatch for bit_width=4");
  }

  @Test
  public void testRoundTripU32BitWidth10() {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i % 1024; // 0..1023, fits in 10 bits
    }
    int[] transposed = new int[1024];
    for (int row = 0; row < 32; row++) {
      for (int lane = 0; lane < 32; lane++) {
        int idx = FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane;
        transposed[idx] = values[row * 32 + lane];
      }
    }

    int[] packed = packU32(10, transposed);
    int[] unpacked = new int[1024];
    FastLanesBitPacking.unpackU32(10, packed, 0, unpacked, 0);

    int[] result = new int[1024];
    for (int row = 0; row < 32; row++) {
      for (int lane = 0; lane < 32; lane++) {
        int idx = FL_ORDER[row / 8] * 16 + (row % 8) * 128 + lane;
        result[row * 32 + lane] = unpacked[idx];
      }
    }

    assertArrayEquals(values, result, "Round-trip mismatch for bit_width=10");
  }

  @Test
  public void testRoundTripU32BitWidth32() {
    int[] values = new int[1024];
    for (int i = 0; i < 1024; i++) {
      values[i] = i * 12345; // arbitrary values
    }
    // For bit_width=32, FastLanes is identity (no transpose needed in pack)
    int[] packed = packU32(32, values);
    int[] unpacked = new int[1024];
    FastLanesBitPacking.unpackU32(32, packed, 0, unpacked, 0);
    assertArrayEquals(values, unpacked, "Round-trip mismatch for bit_width=32");
  }
}
