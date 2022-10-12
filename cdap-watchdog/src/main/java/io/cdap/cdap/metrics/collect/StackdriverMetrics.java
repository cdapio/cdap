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

package io.cdap.cdap.metrics.collect;

import com.google.api.Metric;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.inject.Inject;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import io.cdap.cdap.api.metrics.MetricValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StackdriverMetrics extends AggregatedMetricsCollectionService {
  private static final Logger LOG = LoggerFactory.getLogger(StackdriverMetrics.class);

  private static final String projectId = "ardekani-cdf-sandbox2";
  private MetricServiceClient metricServiceClient;

  @Inject
  public StackdriverMetrics() throws IOException {
    super(1);
    this.metricServiceClient = MetricServiceClient.create();
    LOG.error(">>>> LOADING StackdriverMetrics");
  }

  @Override
  protected void publish(Iterator<MetricValues> metrics) throws Exception {
    LOG.error(">>>> Publishing metrics");
    List<TimeSeries> timeSeriesList = new ArrayList<>();
    ProjectName name = ProjectName.of(projectId);


    Map<String, String> resourceLabels = new HashMap<>();
    resourceLabels.put("project_id", projectId);

    MonitoredResource resource =
      MonitoredResource.newBuilder().setType("global").putAllLabels(resourceLabels).build();

    List<Point> pointList = new ArrayList<>();

    while (metrics.hasNext()) {
      MetricValues metricValues = metrics.next();

      Metric metric =
        Metric.newBuilder()
          .setType("custom.googleapis.com/stores/daily_sales")
          .putAllLabels(metricValues.getTags())
          .build();

      TimeInterval interval =
        TimeInterval.newBuilder()
          .build();

      for (MetricValue value : metricValues.getMetrics()) {
        //TODO: Distribution
        if (value.getType() == MetricType.GAUGE || value.getType() == MetricType.COUNTER) {

          TypedValue tValue = TypedValue.newBuilder().setDoubleValue(value.getValue()).build();
          Point point = Point.newBuilder().setInterval(interval).setValue(tValue).build();
          pointList.add(point);
        }
      }

      TimeSeries timeSeries =
        TimeSeries.newBuilder()
          .setMetric(metric)
          .setResource(resource)
          .addAllPoints(pointList)
          .build();


      timeSeriesList.add(timeSeries);
    }

    CreateTimeSeriesRequest request =
      CreateTimeSeriesRequest.newBuilder()
        .setName(name.toString())
        .addAllTimeSeries(timeSeriesList)
        .build();

    metricServiceClient.createTimeSeries(request);

    LOG.error(">>>> metrics published");
    System.out.printf("Done writing time series data.%n");
  }
}
