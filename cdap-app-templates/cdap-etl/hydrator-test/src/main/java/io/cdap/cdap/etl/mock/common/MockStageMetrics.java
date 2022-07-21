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

package io.cdap.cdap.etl.mock.common;

import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.etl.api.StageMetrics;

import java.util.Map;

/**
 * Mock StageMetrics for unit tests
 */
public class MockStageMetrics implements StageMetrics {
  private final String stageName;
  private final MockMetrics mockMetrics;

  public MockStageMetrics(String stageName) {
    this.stageName = stageName;
    this.mockMetrics = new MockMetrics();
  }

  @Override
  public void count(String s, int i) {
    mockMetrics.count(stageName + "." + s, i);
  }

  @Override
  public void countLong(String s, long l) {
    mockMetrics.countLong(stageName + "." + s, l);
  }

  @Override
  public void gauge(String s, long l) {
    mockMetrics.gauge(stageName + "." + s, l);
  }

  @Override
  public Metrics child(Map<String, String> tags) {
    return mockMetrics.child(tags);
  }

  @Override
  public Map<String, String> getTags() {
    return mockMetrics.getTags();
  }

  @Override
  public void pipelineCount(String s, int i) {
    mockMetrics.count(s, i);
  }

  @Override
  public void pipelineGauge(String s, long l) {
    mockMetrics.gauge(s, l);
  }

  public long getPipelineCount(String s) {
    return mockMetrics.getCount(s);
  }

  public long getPipelineGauge(String s) {
    return mockMetrics.getGauge(s);
  }

  public long getCount(String s) {
    return mockMetrics.getCount(stageName + "." + s);
  }

  public long getGauge(String s) {
    return mockMetrics.getGauge(stageName + "." + s);
  }
}
