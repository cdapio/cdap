/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import java.util.Arrays;

/**
 * Represents a row key from a data set changed as part of a transaction.
 */
public final class ChangeId {
  private final byte[] key;
  private final int hash;

  public ChangeId(byte[] bytes) {
    key = bytes;
    hash = Arrays.hashCode(bytes);
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
    return hash == other.hash && Arrays.equals(key, other.key);
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public String toString() {
    return toStringBinary(key, 0, key.length);
  }

  // Copy from Bytes.toStringBinary so that we don't need direct dependencies on Bytes.
  private String toStringBinary(byte [] b, int off, int len) {
    StringBuilder result = new StringBuilder();
    for (int i = off; i < off + len; ++i) {
      int ch = b[i] & 0xFF;
      if ((ch >= '0' && ch <= '9')
       || (ch >= 'A' && ch <= 'Z')
       || (ch >= 'a' && ch <= 'z')
       || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0) {
        result.append((char) ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }
}
