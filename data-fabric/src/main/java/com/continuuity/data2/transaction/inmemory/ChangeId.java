package com.continuuity.data2.transaction.inmemory;

import com.continuuity.api.common.Bytes;

/**
*
*/
public final class ChangeId {
  private final byte[] key;
  private final int hash;

  public ChangeId(byte[] bytes) {
    key = bytes;
    hash = Bytes.hashCode(key);
  }

  public byte[] getKey() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != ChangeId.class) {
      return false;
    }
    ChangeId other = (ChangeId) o;
    return hash == other.hash && Bytes.equals(key, other.key);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(key);
  }
}
