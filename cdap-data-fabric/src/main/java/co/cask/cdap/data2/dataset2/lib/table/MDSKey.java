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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;

/**
 * Metadata entry key
 */
public final class MDSKey {
  private final byte[] key;

  public MDSKey(byte[] key) {
    this.key = key;
  }

  public byte[] getKey() {
    return key;
  }

  /**
   * Splits the keys into the parts that comprise this.
   */
  public Splitter split() {
    return new Splitter(key);
  }

  /**
   * Decodes the keys parts, as specified (int, long, byte[], String)
   */
  public static final class Splitter {
    private final ByteBuffer byteBuffer;
    private Splitter(byte[] bytes) {
      byteBuffer = ByteBuffer.wrap(bytes);
    }

    public int getInt() {
      return byteBuffer.getInt();
    }

    public long getLong() {
      return byteBuffer.getLong();
    }

    public byte[] getBytes() {
      int len = byteBuffer.getInt();
      byte[] bytes = new byte[len];
      byteBuffer.get(bytes, 0, len);
      return bytes;
    }

    public String getString() {
      return Bytes.toString(getBytes());
    }

    public void skipInt() {
      forward(Ints.BYTES);
    }

    public void skipLong() {
      forward(Longs.BYTES);
    }

    public void skipBytes() {
      int len = byteBuffer.getInt();
      forward(len);
    }

    public void skipString() {
      skipBytes();
    }

    private void forward(int count) {
      int position = byteBuffer.position();
      byteBuffer.position(position + count);
    }
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

    // Encodes parts of the key with segments of <length> <value>
    public Builder add(byte[] part) {
      key = Bytes.add(key, Bytes.toBytes(part.length), part);
      return this;
    }

    public Builder add(String part) {
      add(Bytes.toBytes(part));
      return this;
    }

    public Builder add(String... parts) {
      for (String part : parts) {
        add(part);
      }
      return this;
    }

    public Builder add(long part) {
      key = Bytes.add(key, Bytes.toBytes(part));
      return this;
    }

    public Builder add(int part) {
      key = Bytes.add(key, Bytes.toBytes(part));
      return this;
    }

    public MDSKey build() {
      return new MDSKey(key);
    }
  }
}
