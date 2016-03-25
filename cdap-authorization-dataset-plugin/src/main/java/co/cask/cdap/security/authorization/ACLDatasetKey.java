/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.common.Bytes;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Class to generate and manage keys for {@link ACLDataset}.
 */
final class ACLDatasetKey {
  private final byte[] key;

  /**
   * @param key bytearray as constructed by {@link Builder}
   */
  public ACLDatasetKey(byte[] key) {
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
     * Returns the next {@link String} part in the {@link Splitter}.
     *
     * @throws BufferUnderflowException if there is no String as expected
     * @return the next String part in the splitter
     */
    public String getString() {
      return Bytes.toString(getBytes());
    }

    /**
     * Skips the next {@link String} part in the splitter.
     *
     * @throws BufferUnderflowException if there is no String as expected
     */
    public void skipString() {
      skipBytes();
    }

    /**
     * Returns the next byte[] part in the {@link Splitter}.
     *
     * @throws BufferUnderflowException if there is no byte[] as expected
     * @return the next byte[] part in the {@link Splitter}
     */
    private byte[] getBytes() {
      int len = byteBuffer.getInt();
      if (byteBuffer.remaining() < len) {
        throw new BufferUnderflowException();
      }
      byte[] bytes = new byte[len];
      byteBuffer.get(bytes, 0, len);
      return bytes;
    }

    /**
     * Skips the next byte[] part in the {@link Splitter}.
     *
     * @throws BufferUnderflowException if there is no byte[] as expected
     */
    private void skipBytes() {
      int len = byteBuffer.getInt();
      int position = byteBuffer.position();
      byteBuffer.position(position + len);
    }
  }

  /**
   * Builds an {@link ACLDatasetKey}.
   */
  public static final class Builder {
    private byte[] key;

    public Builder() {
      key = new byte[0];
    }

    public Builder add(String part) {
      add(Bytes.toBytes(part));
      return this;
    }

    public ACLDatasetKey build() {
      return new ACLDatasetKey(key);
    }

    // Encodes parts of the key with segments of <length> <value>
    private Builder add(byte[] part) {
      key = Bytes.add(key, Bytes.toBytes(part.length), part);
      return this;
    }
  }
}
