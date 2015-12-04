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

package co.cask.cdap.api.metrics;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A no-op implementation of {@link MetricsContext}.
 */
public final class NoopMetricsContext implements MetricsContext {

  private final Map<String, String> tags;

  public NoopMetricsContext() {
    this(ImmutableMap.<String, String>of());
  }

  public NoopMetricsContext(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public MetricsContext childContext(Map<String, String> tags) {
    return new NoopMetricsContext(ImmutableMap.<String, String>builder().putAll(this.tags).putAll(tags).build());
  }

  @Override
  public MetricsContext childContext(String tagName, String tagValue) {
    return childContext(ImmutableMap.of(tagName, tagValue));
  }

  @Override
  public Map<String, String> getTags() {
    return tags;
  }

  @Override
  public void increment(String metricName, long value) {
    // no-op
  }

  @Override
  public void gauge(String metricName, long value) {
    // no-op
  }
}
