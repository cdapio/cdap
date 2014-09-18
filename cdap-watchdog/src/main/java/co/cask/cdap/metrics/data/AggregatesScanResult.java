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

import com.google.common.base.Objects;

/**
 *
 */
public final class AggregatesScanResult {

  private final String context;
  private final String metric;
  private final String runId;
  private final String tag;
  private final long value;

  AggregatesScanResult(String context, String metric, String runId, String tag, long value) {
    this.context = context;
    this.metric = metric;
    this.runId = runId;
    this.tag = tag;
    this.value = value;
  }

  public String getContext() {
    return context;
  }

  public String getMetric() {
    return metric;
  }

  public String getRunId() {
    return runId;
  }

  public String getTag() {
    return tag;
  }

  public long getValue() {
    return value;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("context", context)
      .add("metric", metric)
      .add("runId", runId)
      .add("tag", tag)
      .add("value", value)
      .toString();
  }
}
