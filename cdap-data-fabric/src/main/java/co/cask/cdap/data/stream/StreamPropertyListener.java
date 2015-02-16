/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.proto.Id;

/**
 * Listener for changes in stream properties.
 */
public abstract class StreamPropertyListener {

  /**
   * Invoked when stream generation changed. Generation only increase monotonically, hence this method
   * is guaranteed to see only increasing generation across multiple calls.
   *
   * @param streamId Id of the stream
   * @param generation The generation id updated to.
   */
  public void generationChanged(Id.Stream streamId, int generation) {
    // Default no-op
  }

  /**
   * Invoked when the stream generation property is deleted.
   *
   * @param streamId Id of the stream
   */
  public void generationDeleted(Id.Stream streamId) {
    // Default no-op
  }

  /**
   * Invoked when the stream TTL property is changed.
   *
   * @param streamId Id of the stream
   * @param ttl TTL of the stream
   */
  public void ttlChanged(Id.Stream streamId, long ttl) {
    // Default no-op
  }

  /**
   * Invoked when the stream TTL property is deleted.
   *
   * @param streamId Id of the stream
   */
  public void ttlDeleted(Id.Stream streamId) {
    // Default no-op
  }

  /**
   * Invoked when the stream Notification threshold property is changed.
   *
   * @param streamId Id of the stream
   * @param threshold Notification threshold of the stream
   */
  public void thresholdChanged(Id.Stream streamId, int threshold) {
    // Default no-op
  }
}
