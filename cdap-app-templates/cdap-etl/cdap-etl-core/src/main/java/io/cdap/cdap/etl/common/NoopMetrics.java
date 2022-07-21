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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.StageMetrics;

import java.util.Collections;
import java.util.Map;

/**
 * No op metrics implementation for tests.
 */
public class NoopMetrics implements StageMetrics {

  public static final StageMetrics INSTANCE = new NoopMetrics();

  @Override
  public void count(String s, int i) {
    // no-op
  }

  @Override
  public void countLong(String s, long l) {
    // no-op
  }

  @Override
  public void gauge(String s, long l) {
    // no-op
  }

  @Override
  public Metrics child(Map<String, String> tags) {
    return this;
  }

  @Override
  public Map<String, String> getTags() {
    return Collections.emptyMap();
  }

  @Override
  public void pipelineCount(String metricName, int delta) {
    // no-op
  }

  @Override
  public void pipelineGauge(String metricName, long value) {
    // no-op
  }
}
