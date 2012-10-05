package com.continuuity.data.util;

import com.continuuity.common.utils.Bytes;

/**
 * A simple interface that performs a single map() operation on a byte array.
 * 
 * The intended use is to provide pluggable modifiers for functionality like row
 * key hash-prefixing.
 */
public interface KeyMapper {

  public byte[] map(byte[] key);

  public static class HashPrefixer implements KeyMapper {
    @Override
    public byte[] map(byte[] key) {
      return Bytes.add(Bytes.toBytes(Bytes.hashCode(key)), key);
    }
  }
}
