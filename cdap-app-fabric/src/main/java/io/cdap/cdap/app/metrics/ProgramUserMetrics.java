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

package io.cdap.cdap.app.metrics;

import com.google.common.collect.Sets;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.metrics.MetricsContext;
import io.cdap.cdap.common.conf.Constants;

import java.util.Map;

/**
 * Implementation of {@link Metrics} for user-defined metrics.
 * Metrics will be emitted through {@link MetricsCollectionService}.
 */
public class ProgramUserMetrics implements Metrics {

  private final MetricsContext metricsContext;

  public ProgramUserMetrics(MetricsContext metricsContext) {
    this(metricsContext, true);
  }

  public ProgramUserMetrics(MetricsContext metricsContext, boolean addUserScope) {
    if (addUserScope) {
      this.metricsContext = metricsContext.childContext(Constants.Metrics.Tag.SCOPE, "user");
    } else {
      this.metricsContext = metricsContext;
    }
  }

  @Override
  public void count(String metricName, int delta) {
    metricsContext.increment(metricName, delta);
  }

  @Override
  public void gauge(String metricName, long value) {
    metricsContext.gauge(metricName, value);
  }

  @Override
  public Metrics child(Map<String, String> tags) {
    Sets.SetView<String> intersection = Sets.intersection(getTags().keySet(), tags.keySet());
    if (!intersection.isEmpty()) {
      throw new IllegalArgumentException(String.format("Tags with names '%s' already exists in the context. " +
                                                         "Child Metrics cannot be created with duplicate tag names.",
                                                       String.join(", ", intersection)));
    }
    return new ProgramUserMetrics(metricsContext.childContext(tags), false);
  }

  @Override
  public Map<String, String> getTags() {
    return metricsContext.getTags();
  }
}
