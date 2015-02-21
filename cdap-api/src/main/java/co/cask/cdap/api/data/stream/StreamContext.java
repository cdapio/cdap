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

import co.cask.cdap.api.stream.StreamEventData;

import java.util.List;
import java.util.Map;

/**
 * Stream Context.
 */
public interface StreamContext {

  /**
   * Write to Stream
   * @param stream stream id
   * @param data stream data
   * @return status of write to stream
   */
  public StreamWriteStatus writeToStream(String stream, StreamEventData data);

  /**
   * Write to Stream
   * @param stream stream id
   * @param data data
   * @return status of write to stream
   */
  public StreamWriteStatus writeToStream(String stream, byte[] data);

  /**
   * Write to Stream with Headers
   * @param stream stream id
   * @param data data
   * @param headers map of headers
   * @return status of write to stream
   */
  public StreamWriteStatus writeToStream(String stream, byte[] data, Map<String, String> headers);

  /**
   * Write to Stream
   * @param stream stream id
   * @param data data
   * @return status of write to stream
   */
  public StreamWriteStatus writeToStream(String stream, List<byte[]> data);

  /**
   * Write to Stream
   * @param stream stream id
   * @param data data
   * @return status of write to stream
   */
  public StreamWriteStatus writeToStream(String stream, List<byte[]> data, Map<String, String> headers);
}
