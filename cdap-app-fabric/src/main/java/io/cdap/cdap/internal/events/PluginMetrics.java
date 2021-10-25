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

package io.cdap.cdap.internal.events;

public class PluginMetrics {

  String pluginId;
  String pluginType;
  String metricName;
  long metricValue;

  public PluginMetrics(String pluginId, String pluginType, String metricName, long metricValue) {
    this.pluginId = pluginId;
    this.pluginType = pluginType;
    this.metricName = metricName;
    this.metricValue = metricValue;
  }

  public String getPluginId() {
    return pluginId;
  }

  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  public String getPluginType() {
    return pluginType;
  }

  public void setPluginType(String pluginType) {
    this.pluginType = pluginType;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public long getMetricValue() {
    return metricValue;
  }

  public void setMetricValue(long metricValue) {
    this.metricValue = metricValue;
  }
}
