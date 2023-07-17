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

package io.cdap.cdap.master.spi.autoscaler;

/**
 * Creates an object for autoscaler metrics
 */

public class AutoscalingConfig {
    private int minReplicaCount;
    private int maxReplicaCount;
    private String desiredAverageMetricValue;
    private String metricName;
    private int stabilizationWindowTime;
    private int periodTime;
    private int podUpdateCount;

    public AutoscalingConfig(String metricName, int minReplicaCount, int maxReplicaCount,
                             String desiredAverageMetricValue, int stabilizationWindowTime,
                             int periodTime, int podUpdateCount) {
        this.metricName = metricName;
        this.minReplicaCount = minReplicaCount;
        this.maxReplicaCount = maxReplicaCount;
        this.desiredAverageMetricValue = desiredAverageMetricValue;
        this.stabilizationWindowTime = stabilizationWindowTime;
        this.periodTime = periodTime;
        this.podUpdateCount = podUpdateCount;
    }

    public String getMetricName(){
        return this.metricName;
    }

    public int getMinReplicaCount(){
        return this.minReplicaCount;
    }

    public int getMaxReplicaCount(){
        return this.maxReplicaCount;
    }

    public String getDesiredAverageMetricValue(){
        return this.desiredAverageMetricValue;
    }

    public int getStabilizationWindowTime(){
        return this.stabilizationWindowTime;
    }

    public int getPeriodTime(){
        return this.periodTime;
    }

    public int getPodUpdateCount(){
        return this.podUpdateCount;
    }
}
