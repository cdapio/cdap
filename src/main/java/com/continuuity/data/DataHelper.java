package com.continuuity.data;

import org.apache.hadoop.hbase.util.Bytes;

public class DataHelper {

  /**
   * Generates a 4-byte hash of the specified key and returns a copy of the
   * specified key with the hash prepended to it.
   * @param key
   * @return 4-byte-hash(key) + key
   */
  public static byte[] prependHash(byte [] key) {
    byte [] hash = Bytes.toBytes(Bytes.hashCode(key));
    return Bytes.add(hash, key);
  }
}
