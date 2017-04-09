/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.common.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Interface for encoding data.
 */
public interface Encoder {

  /**
   * Writes out a null value.
   *
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeNull() throws IOException;

  /**
   * Writes out a boolean value.
   *
   * @param b the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeBool(boolean b) throws IOException;

  /**
   * Writes out a int value.
   *
   * @param i the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeInt(int i) throws IOException;

  /**
   * Writes out a long value.
   *
   * @param l the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeLong(long l) throws IOException;

  /**
   * Writes out a float value.
   *
   * @param f the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeFloat(float f) throws IOException;

  /**
   * Writes out a double value.
   *
   * @param d the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeDouble(double d) throws IOException;

  /**
   * Writes out a string value.
   *
   * @param s the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeString(String s) throws IOException;

  /**
   * Writes out a byte array.
   *
   * @param bytes the value to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeBytes(byte[] bytes) throws IOException;

  /**
   * Writes out a byte array.
   *
   * @param bytes the value to write
   * @param off the starting offset in the byte array to write
   * @param len number of bytes to write, starting from the given offset
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeBytes(byte[] bytes, int off, int len) throws IOException;

  /**
   * Writes out the remaining bytes in {@link ByteBuffer}.
   * The given {@link ByteBuffer} is untounch after this method is returned (i.e. same position and limit).
   *
   * @param bytes bytes to write
   * @return this Encoder
   * @throws IOException if failed to encode
   */
  Encoder writeBytes(ByteBuffer bytes) throws IOException;
}
