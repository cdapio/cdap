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

package co.cask.cdap.common.metrics;

import com.google.common.util.concurrent.AbstractIdleService;

import java.util.Map;

/**
 * No-op, to be used in unit-tests
 */
public class NoOpMetricsCollectionService extends AbstractIdleService implements MetricsCollectionService {

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // no-op
  }

  @Override
  public MetricsCollector getCollector(Map<String, String> tags) {
    return new MetricsCollector() {
      @Override
      public void increment(String metricName, long value) {
        // no-op
      }

      @Override
      public void gauge(String metricName, long value) {
        // no-op
      }

      @Override
      public MetricsCollector childCollector(Map<String, String> tags) {
        return this;
      }

      @Override
      public MetricsCollector childCollector(String tagName, String tagValue) {
        return this;
      }
    };
  }
}
