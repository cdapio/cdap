/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import io.cdap.cdap.api.common.Bytes;
import java.io.ByteArrayInputStream;

/**
 * A {@link ByteArrayInputStream} that can reset its byte buffer.
 */
public class ResettableByteArrayInputStream extends ByteArrayInputStream {

  public ResettableByteArrayInputStream() {
    super(Bytes.EMPTY_BYTE_ARRAY);
  }

  /**
   * Sets the buffer and resets internal counters.
   *
   * @param buf the new buffer
   */
  public void reset(byte[] buf) {
    this.buf = buf;
    this.pos = 0;
    this.count = buf.length;
    this.mark = 0;
  }
}
