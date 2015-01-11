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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.discovery.Discoverable;

/**
 * Manages Streams leaders. One Stream handler leader is allocated to each Stream.
 * The leader can perform any global processing on the stream, such as aggregating
 * the size of the stream as given by each individual stream writers.
 * The {@link Service#start} method of this class affects a leader to all existing Streams at CDAP startup.
 */
public interface StreamLeaderManager extends Service {

  void setHandlerDiscoverable(Discoverable discoverable);

  /**
   * Affect a Stream handler leader to a stream.
   *
   * @param streamName name of the stream.
   * @return A {@link ListenableFuture} describing the progress of the operation.
   */
  ListenableFuture<Void> affectLeader(String streamName);
}
