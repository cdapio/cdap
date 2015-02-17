/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.common.Bytes;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

/**
 * Metadata entry key
 */
public final class MDSKey {
  private final byte[] key;

  MDSKey(byte[] key) {
    this.key = key;
  }

  public byte[] getKey() {
    return key;
  }

  /**
   * Splits the keys into the parts that comprise this.
   */
  public List<byte[]> split() {
    List<byte[]> bytes = Lists.newArrayList();
    int offset = 0;
    while (offset < key.length) {
      int length = Bytes.toInt(key, offset);
      offset += Ints.BYTES;
      bytes.add(Arrays.copyOfRange(key, offset, offset + length));
      offset += length;
    }
    return bytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MDSKey that = (MDSKey) o;
    return Bytes.equals(this.key, that.key);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(key);
  }

  /**
   * Builds {@link MDSKey}s.
   */
  public static final class Builder {
    private byte[] key;

    public Builder() {
      key = new byte[0];
    }

    public Builder(MDSKey start) {
      this.key = start.getKey();
    }

    public Builder add(byte[] part) {
      key = Bytes.add(key, part);
      return this;
    }

    public Builder add(String part) {
      byte[] b = Bytes.toBytes(part);
      key = Bytes.add(key, Bytes.toBytes(b.length), b);
      return this;
    }

    public Builder add(String... parts) {
      for (String part : parts) {
        add(part);
      }
      return this;
    }

    public Builder add(long part) {
      add(Bytes.toBytes(part));
      return this;
    }

    public MDSKey build() {
      return new MDSKey(key);
    }
  }
}
