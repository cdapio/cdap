/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricSearchQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.api.metrics.MetricsSystemClient;
import co.cask.cdap.api.metrics.TagValue;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link MetricsSystemClient} that operates on {@link MetricStore} directly.
 */
public class DirectMetricsSystemClient implements MetricsSystemClient {

  private final MetricStore metricStore;

  @Inject
  DirectMetricsSystemClient(MetricStore metricStore) {
    this.metricStore = metricStore;
  }

  @Override
  public void delete(MetricDeleteQuery deleteQuery) {
    metricStore.delete(deleteQuery);
  }

  @Override
  public Collection<MetricTimeSeries> query(int start, int end, int resolution, Map<String, String> tags,
                                            Collection<String> metrics, Collection<String> groupByTags) {
    Map<String, AggregationFunction> metricsMap = metrics.stream()
      .collect(Collectors.toMap(m -> m, m -> AggregationFunction.SUM));
    return metricStore.query(new MetricDataQuery(start, end, resolution, Integer.MAX_VALUE,
                                                 metricsMap, tags, new ArrayList<>(groupByTags), null));
  }

  @Override
  public Collection<String> search(Map<String, String> tags) {
    List<TagValue> tagValues = tags.entrySet().stream()
      .map(e -> new TagValue(e.getKey(), e.getValue()))
      .collect(Collectors.toList());
    return metricStore.findMetricNames(new MetricSearchQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, tagValues));
  }
}
