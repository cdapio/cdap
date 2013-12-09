/**
 * Copyright 2010 Sematext International
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.continuuity.hbase.wd;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Provides handy methods to distribute
 */
public class RowKeyDistributorByHashPrefix extends AbstractRowKeyDistributor {
  private static final String DELIM = "--";
  private Hasher hasher;

  /** Constructor reflection. DO NOT USE */
  public RowKeyDistributorByHashPrefix() {
  }

  public RowKeyDistributorByHashPrefix(Hasher hasher) {
    this.hasher = hasher;
  }

  /**
   * todo
   */
  public static interface Hasher extends Parametrizable {
    byte[] getHashPrefix(byte[] originalKey);
    byte[][] getAllPossiblePrefixes();
    int getPrefixLength(byte[] adjustedKey);
  }

  /**
   * todo
   */
  public static class OneByteSimpleHash implements Hasher {
    private int mod;

    /**
     * For reflection, do NOT use it.
     */
    public OneByteSimpleHash() {}

    /**
     * Creates a new instance of this class.
     * @param maxBuckets max buckets number, should be in 1...255 range
     */
    public OneByteSimpleHash(int maxBuckets) {
      if (maxBuckets < 1 || maxBuckets > 256) {
        throw new IllegalArgumentException("maxBuckets should be in 1..256 range");
      }
      // i.e. "real" maxBuckets value = maxBuckets or maxBuckets-1
      this.mod = maxBuckets;
    }

    // Used to minimize # of created object instances
    // Should not be changed. TODO: secure that
    private static final byte[][] PREFIXES;

    static {
      PREFIXES = new byte[256][];
      for (int i = 0; i < 256; i++) {
        PREFIXES[i] = new byte[] {(byte) i};
      }
    }

    @Override
    public byte[] getHashPrefix(byte[] originalKey) {
      long hash = Math.abs(hashBytes(originalKey));
      return new byte[] {(byte) (hash % mod)};
    }

    @Override
    public byte[][] getAllPossiblePrefixes() {
      return Arrays.copyOfRange(PREFIXES, 0, mod);
    }

    @Override
    public int getPrefixLength(byte[] adjustedKey) {
      return 1;
    }

    @Override
    public String getParamsToStore() {
      return String.valueOf(mod);
    }

    @Override
    public void init(String storedParams) {
      this.mod = Integer.valueOf(storedParams);
    }

    /** Compute hash for binary data. */
    private static int hashBytes(byte[] bytes) {
      int hash = 1;
      for (int i = 0; i < bytes.length; i++) {
        hash = (31 * hash) + (int) bytes[i];
      }
      return hash;
    }
  }

  @Override
  public byte[] getDistributedKey(byte[] originalKey) {
    return Bytes.add(hasher.getHashPrefix(originalKey), originalKey);
  }

  @Override
  public byte[] getOriginalKey(byte[] adjustedKey) {
    int prefixLength = hasher.getPrefixLength(adjustedKey);
    if (prefixLength > 0) {
      return Bytes.tail(adjustedKey, adjustedKey.length - prefixLength);
    } else {
      return adjustedKey;
    }
  }

  @Override
  public byte[][] getAllDistributedKeys(byte[] originalKey) {
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    byte[][] keys = new byte[allPrefixes.length][];
    for (int i = 0; i < allPrefixes.length; i++) {
      keys[i] = Bytes.add(allPrefixes[i], originalKey);
    }

    return keys;
  }

  @Override
  public String getParamsToStore() {
    String hasherParamsToStore = hasher.getParamsToStore();
    return hasher.getClass().getName() + DELIM + (hasherParamsToStore == null ? "" : hasherParamsToStore);
  }

  @Override
  public void init(String params) {
    String[] parts = params.split(DELIM, 2);
    try {
      this.hasher = (Hasher) Class.forName(parts[0]).newInstance();
      this.hasher.init(parts[1]);
    } catch (Exception e) {
      throw new RuntimeException("RoKeyDistributor initialization failed", e);
    }
  }
}
