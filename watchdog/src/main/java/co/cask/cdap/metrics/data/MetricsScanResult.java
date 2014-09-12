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

package co.cask.cdap.metrics.data;

import java.util.Iterator;

/**
 *
 */
public final class MetricsScanResult implements Iterable<TimeValue> {

  private final String context;
  private final String runId;
  private final String metric;
  private final String tag;
  private final Iterable<TimeValue> timeValues;

  public MetricsScanResult(String context, String runId, String metric, String tag, Iterable<TimeValue> timeValues) {
    this.context = context;
    this.runId = runId;
    this.metric = metric;
    this.tag = tag;
    this.timeValues = timeValues;
  }

  public String getContext() {
    return context;
  }

  public String getRunId() {
    return runId;
  }

  public String getMetric() {
    return metric;
  }

  public String getTag() {
    return tag;
  }

  @Override
  public Iterator<TimeValue> iterator() {
    return timeValues.iterator();
  }
}
