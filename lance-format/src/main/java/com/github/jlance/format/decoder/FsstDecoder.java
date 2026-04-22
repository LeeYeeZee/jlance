// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package com.github.jlance.format.decoder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decoder for Lance's FSST (Fast Static Symbol Table) string compression.
 *
 * <p>This implementation mirrors the Rust {@code fsst::fsst::decompress} function used by
 * Lance V2.1+.  FSST is a lightweight encoding for variable-width data that learns a small
 * symbol table (≤255 symbols, 1–8 bytes each) per page and stores it in the encoding
 * description.
 *
 * <p>The symbol table layout is:
 * <pre>
 *   [header: u64 LE]                    // FSST_MAGIC | decoder_switch_on | symbol_num
 *   [symbols: 256 × u64 LE]             // symbol content (up to 8 bytes each)
 *   [lengths: 256 × u8]                 // symbol lengths (1–8)
 * </pre>
 *
 * <p>A symbol table is always {@value #SYMBOL_TABLE_SIZE} bytes.  If {@code decoder_switch_on}
 * is off the input is copied verbatim (FSST was skipped because the input was too small).
 */
public final class FsstDecoder {

  private FsstDecoder() {}

  /** FSST magic number in the high 32 bits of the symbol-table header. */
  private static final long FSST_MAGIC = 0x46535354L << 32;

  /** Escape code – the next byte is emitted verbatim. */
  private static final int ESCAPE_CODE = 255;

  /** Total size of a Lance FSST symbol table (bytes). */
  public static final int SYMBOL_TABLE_SIZE = 8 + 256 * 8 + 256; // 2312

  /**
   * Decompresses FSST-compressed strings.
   *
   * @param symbolTable   the FSST symbol table ({@value #SYMBOL_TABLE_SIZE} bytes)
   * @param inBuf         compressed byte stream (all strings concatenated)
   * @param inOffsets     offsets into {@code inBuf}; length = numStrings + 1
   * @param outBuf        output buffer; must be large enough to hold expanded data
   *                      (Rust uses {@code inBuf.length * 8} as upper bound)
   * @param outOffsets    output offsets; length = numStrings + 1
   * @throws IllegalArgumentException if the symbol table is malformed
   */
  public static void decompress(
      byte[] symbolTable,
      byte[] inBuf,
      int[] inOffsets,
      byte[] outBuf,
      int[] outOffsets) {

    if (symbolTable.length != SYMBOL_TABLE_SIZE) {
      throw new IllegalArgumentException(
          "FSST symbol table must be " + SYMBOL_TABLE_SIZE + " bytes, got " + symbolTable.length);
    }

    long stInfo = ByteBuffer.wrap(symbolTable, 0, 8)
        .order(ByteOrder.LITTLE_ENDIAN).getLong();

    if ((stInfo & FSST_MAGIC) != FSST_MAGIC) {
      throw new IllegalArgumentException("Invalid FSST symbol table magic");
    }

    boolean decoderSwitchOn = (stInfo & (1L << 24)) != 0;
    int symbolNum = (int) (stInfo & 255L);

    long[] symbols = new long[256];
    byte[] lens = new byte[256];

    int pos = 8;
    for (int i = 0; i < symbolNum; i++) {
      symbols[i] = ByteBuffer.wrap(symbolTable, pos, 8)
          .order(ByteOrder.LITTLE_ENDIAN).getLong();
      pos += 8;
    }
    for (int i = 0; i < symbolNum; i++) {
      lens[i] = symbolTable[pos++];
    }

    if (!decoderSwitchOn) {
      // FSST encoder decided the data was too small to compress – copy verbatim.
      System.arraycopy(inBuf, 0, outBuf, 0, inBuf.length);
      System.arraycopy(inOffsets, 0, outOffsets, 0, inOffsets.length);
      return;
    }

    int outPos = 0;
    outOffsets[0] = 0;

    for (int i = 1; i < inOffsets.length; i++) {
      int inCurr = inOffsets[i - 1];
      int inEnd = inOffsets[i];

      while (inCurr < inEnd) {
        int code = inBuf[inCurr++] & 0xFF;
        if (code == ESCAPE_CODE) {
          if (inCurr < inEnd) {
            outBuf[outPos++] = inBuf[inCurr++];
          }
        } else {
          long sym = symbols[code];
          int len = lens[code] & 0xFF;
          for (int b = 0; b < len; b++) {
            outBuf[outPos++] = (byte) (sym & 0xFF);
            sym >>>= 8;
          }
        }
      }
      outOffsets[i] = outPos;
    }
  }
}
