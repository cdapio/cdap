/*
 * Copyright 2014 Cask, Inc.
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
 * Interface for decoding data.
 */
public interface Decoder {
  /**
   * Returns a null value.
   *
   * @return Always returns {@code null}
   * @throws IOException
   */
  Object readNull() throws IOException;

  boolean readBool() throws IOException;

  int readInt() throws IOException;

  long readLong() throws IOException;

  float readFloat() throws IOException;

  double readDouble() throws IOException;

  String readString() throws IOException;

  ByteBuffer readBytes() throws IOException;

  /**
   * Skips a float.
   */
  void skipFloat() throws IOException;

  /**
   * Skips a double.
   */
  void skipDouble() throws IOException;

  /**
   * Skips the a string.
   */
  void skipString() throws IOException;

  /**
   * Skips a byte array.
   */
  void skipBytes() throws IOException;
}
