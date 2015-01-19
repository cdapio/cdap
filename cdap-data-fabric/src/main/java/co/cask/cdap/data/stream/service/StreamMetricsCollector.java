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

import co.cask.cdap.common.metrics.MetricsCollector;

import java.util.Map;

/**
 * Implementation of {@link MetricsCollector} for Streams.
 */
public abstract class StreamMetricsCollector implements MetricsCollector {
  private final MetricsCollector delegate;

  protected StreamMetricsCollector(MetricsCollector delegate) {
    this.delegate = delegate;
  }

  /**
   * Emit stream metrics.
   *
   * @param metricsCollector child {@link MetricsCollector} of the stream to publish metrics about
   * @param bytesWritten number of bytes written to the string
   * @param eventsWritten number of events written to the string
   */
  public abstract void emitMetrics(MetricsCollector metricsCollector, long bytesWritten, long eventsWritten);

  @Override
  public void increment(String metricName, long value) {
    delegate.increment(metricName, value);
  }

  @Override
  public void gauge(String metricName, long value) {
    delegate.gauge(metricName, value);
  }

  @Override
  public MetricsCollector childCollector(Map<String, String> tags) {
    return wrapMetricsCollector(delegate.childCollector(tags));
  }

  @Override
  public StreamMetricsCollector childCollector(String tagName, String tagValue) {
    return wrapMetricsCollector(delegate.childCollector(tagName, tagValue));
  }

  private StreamMetricsCollector wrapMetricsCollector(final MetricsCollector collector) {
    final StreamMetricsCollector current = this;
    return new StreamMetricsCollector(collector) {
      @Override
      public void emitMetrics(MetricsCollector metricsCollector, long bytesWritten, long eventsWritten) {
        current.emitMetrics(collector, bytesWritten, eventsWritten);
      }
    };
  }
}
