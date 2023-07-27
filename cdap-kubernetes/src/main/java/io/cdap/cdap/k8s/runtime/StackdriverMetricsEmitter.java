/*
 * Copyright © 2022-2023 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TypedValue;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.util.Timestamps;
import com.google.api.Metric;
import io.cdap.cdap.master.spi.autoscaler.MetricsEmitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StackdriverClient to emit the metrics to Cloud Monitoring Engine.
 */
public class StackdriverMetricsEmitter implements MetricsEmitter {
    private static final Logger LOG = LoggerFactory.getLogger(StackdriverMetricsEmitter.class);
    private String podName;
    private String podNamespace;
    private String metricName;
    private String projectName;
    private String clusterName;
    private String location;

    StackdriverMetricsEmitter(String podName, String podNamespace){
        this.podName = podName;
        this.podNamespace = podNamespace;
    }

    @Override
    public void setMetricLabels(String metricName, String clusterName, String projectName, String location){
        this.metricName = metricName;
        this.clusterName = clusterName;
        this.projectName = projectName;
        this.location = location;
    }

    @Override
    public void emitMetrics(double metricValue) throws Exception {

        // Instantiates a client
        MetricServiceClient metricServiceClient = MetricServiceClient.create();
        LOG.debug("MetricName: {}, PodName: {}, PodNamespace: {}, MetricValue: {}, ClusterName: {}, " +
                "ProjectName: {}", metricName, podName, podNamespace, metricValue, clusterName, projectName);

        // Prepares an individual data point
        TimeInterval interval =
                TimeInterval.newBuilder()
                        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
                        .build();
        TypedValue value = TypedValue.newBuilder()
                .setDoubleValue(metricValue).build();
        Point point = Point.newBuilder().setInterval(interval).setValue(value).build();

        List<Point> pointList = new ArrayList<>();
        pointList.add(point);

        ProjectName name = ProjectName.of(projectName);

        // Prepares the metric descriptor
        Map<String, String> metricLabels = new HashMap<>();
        Metric metric = Metric.newBuilder()
                .setType("custom.googleapis.com/" + metricName)
                .putAllLabels(metricLabels)
                .build();

        // create MonitoredResource
        Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("location", location);
        resourceLabels.put("cluster_name", clusterName);
        resourceLabels.put("project_id", projectName);
        resourceLabels.put("namespace_name", podNamespace);
        resourceLabels.put("pod_name", podName);
        MonitoredResource resource = MonitoredResource.newBuilder()
                .setType("k8s_pod").putAllLabels(resourceLabels).build();


        // Create time series request
        TimeSeries timeSeries =
                TimeSeries.newBuilder()
                        .setMetric(metric)
                        .setResource(resource)
                        .addAllPoints(pointList)
                        .build();
        List<TimeSeries> timeSeriesList = new ArrayList<>();
        timeSeriesList.add(timeSeries);
        CreateTimeSeriesRequest request = CreateTimeSeriesRequest.newBuilder()
                .setName(name.toString())
                .addAllTimeSeries(timeSeriesList)
                .build();

        // Writes time series data
        metricServiceClient.createTimeSeries(request);


        metricServiceClient.close();
    }
}
