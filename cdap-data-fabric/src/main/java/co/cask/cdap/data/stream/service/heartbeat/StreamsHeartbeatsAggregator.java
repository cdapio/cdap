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

package co.cask.cdap.data.stream.service.heartbeat;

import com.google.common.util.concurrent.Service;

import java.io.IOException;
import java.util.Collection;

/**
 * Aggregator of heartbeats sent by Stream writers.
 */
public interface StreamsHeartbeatsAggregator extends Service {

  /**
   * Perform aggregation on the Streams described by the {@code streamNames}, and no other Streams.
   * If aggregation was previously done on other Streams, those must be cancelled.
   *
   * @param streamNames names of the streams to perform data sizes aggregation on
   */
  void listenToStreams(Collection<String> streamNames);

  /**
   * Perform aggregation on the Stream described by the {@code streamName}.
   * This method does not cancel aggregation done on other Streams.
   * This call does nothing this aggregator already listens to the Stream.
   *
   * @param streamName name of the stream to perform data sizes aggregation on
   * @throws IOException when an error occurred in subscribing to the heartbeats of the stream
   */
  void listenToStream(String streamName) throws IOException;
}
