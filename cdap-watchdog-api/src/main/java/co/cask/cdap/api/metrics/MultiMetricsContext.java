/*
 * Copyright © 2016 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link MetricsContext} that delegates metrics operations to a list of {@link MetricsContext}.
 */
public class MultiMetricsContext implements MetricsContext {

  private final List<MetricsContext> delegates;

  public MultiMetricsContext(MetricsContext...contexts) {
    this(Arrays.asList(contexts));
  }

  public MultiMetricsContext(Iterable<? extends MetricsContext> contexts) {
    this.delegates = ImmutableList.copyOf(contexts);
  }

  @Override
  public MetricsContext childContext(Map<String, String> tags) {
    List<MetricsContext> childContexts = new ArrayList<>(delegates.size());
    for (MetricsContext metricsContext : delegates) {
      childContexts.add(metricsContext.childContext(tags));
    }
    return new MultiMetricsContext(childContexts);
  }

  @Override
  public MetricsContext childContext(String tagName, String tagValue) {
    List<MetricsContext> childContexts = new ArrayList<>(delegates.size());
    for (MetricsContext metricsContext : delegates) {
      childContexts.add(metricsContext.childContext(tagName, tagValue));
    }
    return new MultiMetricsContext(childContexts);
  }

  @Override
  public Map<String, String> getTags() {
    Map<String, String> result = new HashMap<>();
    for (MetricsContext metricsContext : delegates) {
      result.putAll(metricsContext.getTags());
    }
    return Collections.unmodifiableMap(result);
  }

  @Override
  public void increment(String metricName, long value) {
    for (MetricsContext metricsContext : delegates) {
      metricsContext.increment(metricName, value);
    }
  }

  @Override
  public void gauge(String metricName, long value) {
    for (MetricsContext metricsContext : delegates) {
      metricsContext.gauge(metricName, value);
    }
  }
}
