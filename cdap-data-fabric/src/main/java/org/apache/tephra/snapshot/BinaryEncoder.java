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

package org.apache.tephra.snapshot;

import java.io.IOException;
import java.io.OutputStream;

/**
 *  An encoder to help encode snapshots in binary format.
 */
public final class BinaryEncoder {

  private final OutputStream output;

  /**
   * @param output stream to write to
   */
  public BinaryEncoder(OutputStream output) {
    this.output = output;
  }

  /**
   * write a single int value.
   * @throws java.io.IOException If there is IO error.
   */
  public BinaryEncoder writeInt(int i) throws IOException {
    // Compute the zig-zag value. First double the value and flip the bit if the input is negative.
    int val = (i << 1) ^ (i >> 31);

    if ((val & ~0x7f) != 0) {
      output.write(0x80 | val & 0x7f);
      val >>>= 7;
      while (val > 0x7f) {
        output.write(0x80 | val & 0x7f);
        val >>>= 7;
      }
    }
    output.write(val);

    return this;
  }

  /**
   * write a single long int value.
   * @throws java.io.IOException If there is IO error.
   */
  public BinaryEncoder writeLong(long l) throws IOException {
    // Compute the zig-zag value. First double the value and flip the bit if the input is negative.
    long val = (l << 1) ^ (l >> 63);

    if ((val & ~0x7f) != 0) {
      output.write((int) (0x80 | val & 0x7f));
      val >>>= 7;
      while (val > 0x7f) {
        output.write((int) (0x80 | val & 0x7f));
        val >>>= 7;
      }
    }
    output.write((int) val);

    return this;
  }

  /**
   * write a sequence of bytes. First writes the number of bytes as an int, then the bytes themselves.
   * @throws java.io.IOException If there is IO error.
   */
  public BinaryEncoder writeBytes(byte[] bytes) throws IOException {
    writeLong(bytes.length);
    output.write(bytes, 0, bytes.length);
    return this;
  }
}
