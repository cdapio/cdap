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
