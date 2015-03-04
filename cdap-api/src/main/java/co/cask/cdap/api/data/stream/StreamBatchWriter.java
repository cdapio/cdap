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

package co.cask.cdap.api.data.stream;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Used to write to Stream in Batch mode. Once closed, a new {@link StreamBatchWriter} needs to be created to do
 * another set of batch writes.
 */
public interface StreamBatchWriter extends Closeable {

  /**
   * Each call to write will write data to the stream batch endpoint.
   *
   * @param data {@link ByteBuffer}
   * @throws IOException
   */
  void write(ByteBuffer data) throws IOException;

  /**
   * Method is used to complete the current set of batch writes (needs to be invoked after the batch writes
   * are complete).
   *
   * @throws IOException
   */
  void close() throws IOException;
}
