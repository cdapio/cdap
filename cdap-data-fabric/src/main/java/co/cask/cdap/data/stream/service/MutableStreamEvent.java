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

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A mutable {@link StreamEvent} that allows setting the data and timestamp.
 */
@NotThreadSafe
public final class MutableStreamEvent extends StreamEvent {

  private StreamEventData data;
  private long timestamp;

  /**
   * Sets the event data and timestamp.

   * @return this instance
   */
  public MutableStreamEvent set(StreamEventData data, long timestamp) {
    return setData(data).setTimestamp(timestamp);
  }

  /**
   * Sets the event data.
   *
   * @param data the data to set
   * @return this instance
   */
  public MutableStreamEvent setData(StreamEventData data) {
    this.data = data;
    return this;
  }

  /**
   * Sets the event timestamp.
   *
   * @param timestamp the timestamp to set
   * @return this instance
   */
  public MutableStreamEvent setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public ByteBuffer getBody() {
    return data.getBody();
  }

  @Override
  public Map<String, String> getHeaders() {
    return data.getHeaders();
  }
}
