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

import co.cask.cdap.data2.transaction.stream.StreamConfig;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;

/**
 * This class responsible for process coordination needed between stream writers and consumers.
 */
public interface StreamCoordinator extends Service {

  /**
   * Increments the generation of the given stream.
   *
   * @param streamConfig stream configuration
   * @param lowerBound The minimum generation id to increment from. It is guaranteed that the resulting generation
   *                   would be greater than this lower bound value.
   * @return A future that will be completed when the update of generation is done. The future result will carry
   *         the generation id updated by this method.
   */
  ListenableFuture<Integer> nextGeneration(StreamConfig streamConfig, int lowerBound);

  /**
   * Changes the TTL of the given stream.
   *
   * @param streamConfig stream configuration
   * @param ttl the new TTL
   * @return A future that will be completed when the update of TTL is done. The future result will carry
   *         the TTL updated by this method.
   */
  ListenableFuture<Long> changeTTL(StreamConfig streamConfig, long ttl);

  /**
   * Receives event for changes in stream properties.
   *
   * @param listener listener to get called when there is change in stream properties.
   * @return A {@link Cancellable} to cancel the watch
   */
  Cancellable addListener(String streamName, StreamPropertyListener listener);

  /**
   * Called whenever a new stream is created.
   * Affect a Stream handler leader to a stream.
   *
   * @param streamName name of the stream.
   * @return A {@link ListenableFuture} describing the progress of the operation.
   */
  ListenableFuture<Void> streamCreated(String streamName);

  /**
   * Set the {@link Discoverable} that defines the Stream handler in which this {@link StreamCoordinator} runs.
   * This method has to be called before this service is started.
   *
   * @param discoverable discoverable that defines the Stream handler in which this object runs.
   */
  void setHandlerDiscoverable(Discoverable discoverable);
}
