/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction.inmemory;

import com.continuuity.api.common.Bytes;

/**
 * Represents a row key from a data set changed as part of a transaction.
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
