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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Metadata entry key
 */
public final class MDSKey {
  private final byte[] key;

  /**
   * @param key bytearray as constructed by {@code MDSKey.Builder}
   */
  public MDSKey(byte[] key) {
    this.key = key;
  }

  /**
   * @return the underlying key
   */
  public byte[] getKey() {
    return key;
  }

  /**
   * Splits the keys into a {@link Splitter} which exposes the parts that comprise the key.
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

    /**
     * @throws BufferUnderflowException if there is no int as expected
     * @return the next int part in the splitter
     */
    public int getInt() {
      return byteBuffer.getInt();
    }

    /**
     * @throws BufferUnderflowException if there is no long as expected
     * @return the next long part in the splitter
     */
    public long getLong() {
      return byteBuffer.getLong();
    }

    /**
     * @throws BufferUnderflowException if there is no byte[] as expected
     * @return the next byte[] part in the splitter
     */
    public byte[] getBytes() {
      int len = byteBuffer.getInt();
      if (byteBuffer.remaining() < len) {
        throw new BufferUnderflowException();
      }
      byte[] bytes = new byte[len];
      byteBuffer.get(bytes, 0, len);
      return bytes;
    }

    /**
     * @throws BufferUnderflowException if there is no String as expected
     * @return the next String part in the splitter
     */
    public String getString() {
      return Bytes.toString(getBytes());
    }

    /**
     * skips the next int part in the splitter
     * @throws BufferUnderflowException if there is no int as expected
     */
    public void skipInt() {
      forward(Ints.BYTES);
    }

    /**
     * skips the next long part in the splitter
     * @throws BufferUnderflowException if there is no long as expected
     */
    public void skipLong() {
      forward(Longs.BYTES);
    }

    /**
     * skips the next byte[] part in the splitter
     * @throws BufferUnderflowException if there is no byte[] as expected
     */
    public void skipBytes() {
      int len = byteBuffer.getInt();
      forward(len);
    }

    /**
     * skips the next String part in the splitter
     * @throws BufferUnderflowException if there is no String as expected
     */
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
