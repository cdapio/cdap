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

/**
 * Factory for creating {@link StreamStatisticsCollector}s.
 */
public interface StreamStatisticsCollectorFactory {

  /**
   * Collector of statistics for a stream.
   */
  public interface StreamStatisticsCollector {

    /**
     * Emit stream statistics.
     *
     * @param bytesWritten number of bytes written to the stream
     * @param eventsWritten number of events written to the stream
     */
    void emitStatistics(long bytesWritten, long eventsWritten);
  }

  /**
   * Create a {@link StreamStatisticsCollector} for the given {@code streamName}.
   *
   * @param streamName stream name to create a collector for
   * @return a {@link StreamStatisticsCollector} for the given {@code streamName}
   */
  StreamStatisticsCollector createStatisticsCollector(String streamName);

  /**
   * Called when a stream event has been rejected
   */
  void eventRejected();
}
