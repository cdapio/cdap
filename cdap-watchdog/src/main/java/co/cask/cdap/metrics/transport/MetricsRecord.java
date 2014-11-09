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
package co.cask.cdap.metrics.transport;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Record class for carrying information about one metric.
 */
@Nonnull
public final class MetricsRecord {

  private final String context;             // Program context of where the metric get generated.
  private final String runId;               // RunId
  private final String name;                // Name of the metric
  private final List<TagMetric> tags;       // List of TagMetric
  private final long timestamp;             // Timestamp in second of when the metric happened.
  private final long value;                  // Value of the metric, regardless of tags
  private final MetricType type;      // Type of the metric value, to be set(gauge) or to increment

  public MetricsRecord(String context, String runId,
                       String name, Iterable<TagMetric> tags,
                       long timestamp, long value, MetricType type) {
    this.context = context;
    this.runId = runId;
    this.timestamp = timestamp;
    this.name = name;
    this.value = value;
    this.tags = ImmutableList.copyOf(tags);
    this.type = type;
  }

  public String getContext() {
    return context;
  }

  public String getRunId() {
    return runId;
  }

  public String getName() {
    return name;
  }

  public Collection<TagMetric> getTags() {
    return tags;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getValue() {
    return value;
  }

  public MetricType getType() {
    return type;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(MetricsRecord.class)
      .add("context", context)
      .add("runId", runId)
      .add("name", name)
      .add("tags", tags)
      .add("timestamp", timestamp)
      .add("value", value)
      .add("type", type)
      .toString();
  }
}
