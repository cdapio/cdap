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

package co.cask.cdap.data.stream.service;

import co.cask.cdap.data2.transaction.stream.StreamConfig;

import java.io.IOException;

/**
 * Fetches the size of the data persisted by the stream writer in which this interface is run.
 */
public interface StreamWriterSizeFetcher {

  /**
   * Get the size of the data persisted by this Stream writer for the stream which config is the {@code streamConfig}.
   *
   * @param streamConfig stream to get data size of
   * @return the size of the data persisted by this Stream writer for the stream which config is the {@code streamName}
   * @throws IOException in case of any error in fetching the size
   */
  long fetchSize(StreamConfig streamConfig) throws IOException;
}
