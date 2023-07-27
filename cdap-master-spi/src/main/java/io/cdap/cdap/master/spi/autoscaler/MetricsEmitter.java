/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.autoscaler;


/**
 * Interface to emit autoscaler metrics to Cloud Monitoring.
 */

public interface MetricsEmitter {

    /**
     * Emits the autoscaler metrics to Cloud Monitoring
     * @param metricValue the value of the metrics to be emitted.
     */

    void emitMetrics(double metricValue) throws Exception;
    void setMetricLabels(String metricName, String clusterName, String projectName, String location);
}
