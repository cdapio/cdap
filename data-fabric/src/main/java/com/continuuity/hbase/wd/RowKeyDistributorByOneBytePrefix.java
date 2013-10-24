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

/**
 * Provides handy methods to distribute
 */
public class RowKeyDistributorByOneBytePrefix extends AbstractRowKeyDistributor {
  private static final byte[][] PREFIXES;

  static {
    PREFIXES = new byte[Byte.MAX_VALUE][];
    for (byte i = 0; i < Byte.MAX_VALUE; i++) {
      PREFIXES[i] = new byte[] {i};
    }
  }

  private byte maxPrefix;
  private byte nextPrefix;

  /** Constructor reflection. DO NOT USE */
  public RowKeyDistributorByOneBytePrefix() {
  }

  public RowKeyDistributorByOneBytePrefix(byte bucketsCount) {
    this.maxPrefix = bucketsCount;
    this.nextPrefix = 0;
  }

  @Override
  public byte[] getDistributedKey(byte[] originalKey) {
    byte[] key = Bytes.add(PREFIXES[nextPrefix++], originalKey);
    nextPrefix = (byte) (nextPrefix % maxPrefix);

    return key;
  }

  @Override
  public byte[] getOriginalKey(byte[] adjustedKey) {
    return Bytes.tail(adjustedKey, adjustedKey.length - 1);
  }

  @Override
  public byte[][] getAllDistributedKeys(byte[] originalKey) {
    return getAllDistributedKeys(originalKey, maxPrefix);
  }

  private static byte[][] getAllDistributedKeys(byte[] originalKey, byte maxPrefix) {
    byte[][] keys = new byte[maxPrefix][];
    for (byte i = 0; i < maxPrefix; i++) {
      keys[i] = Bytes.add(PREFIXES[i], originalKey);
    }

    return keys;
  }

  @Override
  public String getParamsToStore() {
    return String.valueOf(maxPrefix);
  }

  @Override
  public void init(String params) {
    maxPrefix = Byte.parseByte(params);
  }
}
