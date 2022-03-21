/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.metrics.process.loader;

import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;

import java.io.IOException;
import java.util.Collection;

/**
 * Mock implementation for {@link MetricsWriter} that does no-op
 */
public class MockMetricsWriter implements MetricsWriter {

  private final String id;

  MockMetricsWriter(String id) {
    this.id = id;
  }

  @Override
  public void write(Collection<MetricValues> metricValues) {
    //no-op
  }

  @Override
  public void initialize(MetricsWriterContext metricsWriterContext) {
    //no-op
  }

  @Override
  public String getID() {
    return id;
  }

  @Override
  public void close() throws IOException {
    //no-op
  }
}
