/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.common.utils;

/**
 * Utilities for computing checksums.
 */
public final class Checksums {

  private Checksums() { }

  private static final long EMPTY64 = 0xc15d213aa4d7a795L;

  /**
   * The implementation of fingerprint algorithm is copied from {@code org.apache.avro.SchemaNormalization} class.
   *
   * @param data byte string for which fingerprint is to be computed
   * @return the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a byte string
   */
   public static long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b: data) {
      int index = (int) (result ^ b) & 0xff;
      result = (result >>> 8) ^ FP64.FP_TABLE[index];
    }
    return result;
  }

  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
