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
import javax.annotation.Nullable;

/**
 * Interface for decoding data.
 */
public interface Decoder {

  /**
   * Decodes and return a null value.
   *
   * @return Always returns {@code null}
   * @throws IOException if failed to decode
   */
  @Nullable
  Object readNull() throws IOException;

  /**
   * Decodes and return a boolean value
   *
   * @return the decoded boolean
   * @throws IOException if failed to decode
   */
  boolean readBool() throws IOException;

  /**
   * Decodes and return a int value
   *
   * @return the decoded int
   * @throws IOException if failed to decode
   */
  int readInt() throws IOException;

  /**
   * Decodes and return a long value
   *
   * @return the decoded long
   * @throws IOException if failed to decode
   */
  long readLong() throws IOException;

  /**
   * Decodes and return a float value
   *
   * @return the decoded float
   * @throws IOException if failed to decode
   */
  float readFloat() throws IOException;

  /**
   * Decodes and return a double value
   *
   * @return the decoded double
   * @throws IOException if failed to decode
   */
  double readDouble() throws IOException;

  /**
   * Decodes and return a string value
   *
   * @return the decoded string
   * @throws IOException if failed to decode
   */
  String readString() throws IOException;

  /**
   * Decodes and return bytes
   *
   * @return the decoded bytes
   * @throws IOException if failed to decode
   */
  ByteBuffer readBytes() throws IOException;

  /**
   * Skips a float.
   *
   * @throws IOException if failed to skip
   */
  void skipFloat() throws IOException;

  /**
   * Skips a double.
   *
   * @throws IOException if failed to skip
   */
  void skipDouble() throws IOException;

  /**
   * Skips the a string.
   *
   * @throws IOException if failed to skip
   */
  void skipString() throws IOException;

  /**
   * Skips a byte array.
   *
   * @throws IOException if failed to skip
   */
  void skipBytes() throws IOException;
}
