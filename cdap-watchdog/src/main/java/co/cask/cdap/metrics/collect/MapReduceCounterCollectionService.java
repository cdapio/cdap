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
package co.cask.cdap.metrics.collect;

import co.cask.cdap.api.metrics.MetricType;
import co.cask.cdap.api.metrics.MetricValue;
import co.cask.cdap.api.metrics.MetricValues;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Iterator;
import java.util.Map;

/**
 * A {@link co.cask.cdap.metrics.collect.AggregatedMetricsCollectionService} that publish
 * {@link co.cask.cdap.api.metrics.MetricValues} to MapReduce counters.
 */
@Singleton
public final class MapReduceCounterCollectionService extends AggregatedMetricsCollectionService {
  private final TaskAttemptContext taskContext;

  @Inject
  MapReduceCounterCollectionService(TaskAttemptContext taskContext) {
    this.taskContext = taskContext;
  }

  @Override
  protected void startUp() throws Exception {
    // no-op, but need to override abstract method.
  }


  @Override
  protected void publish(Iterator<MetricValues> metrics) throws Exception {
    while (metrics.hasNext()) {
      MetricValues record = metrics.next();
      publishMetric(record);
    }
  }

  private void publishMetric(MetricValues record) {
    // The format of the counters:
    // * counter group name: "cdap.<tag_name>.<tag_value>[.<tag_name>.<tag_value>[...]]
    // * counter name: metric name

    StringBuilder counterGroup = new StringBuilder("cdap");
    // flatten tags
    for (Map.Entry<String, String> tag : record.getTags().entrySet()) {
      // escape dots with tilde
      counterGroup.append(".").append(tag.getKey()).append(".").append(tag.getValue().replace(".", "~"));
    }
    for (MetricValue metric : record.getMetrics()) {
      String counterName = getCounterName(metric.getName());
      if (metric.getType() == MetricType.COUNTER) {
        taskContext.getCounter(counterGroup.toString(), counterName).increment(metric.getValue());
      } else {
        taskContext.getCounter(counterGroup.toString(), counterName).setValue(metric.getValue());
      }
    }
  }

  public static Map<String, String> parseTags(String counterGroupName) {
    // see publish method for format info

    Preconditions.checkArgument(counterGroupName.startsWith("cdap."),
                                "Counters group was not created by CDAP: " + counterGroupName);
    String[] parts = counterGroupName.split("\\.");
    Map<String, String> tags = Maps.newHashMap();
    // todo: assert that we have odd count of parts?
    for (int i = 1; i < parts.length; i += 2) {
      String tagName = parts[i];
      String tagValue = parts[i + 1];
      // replace tilde with dots as context with dots are escaped with tilde
      tags.put(tagName, tagValue.replace("~", "."));
    }
    return tags;
  }

  private String getCounterName(String metric) {
    return getCounterName(metric, null);
  }

  private String getCounterName(String metric, String tag) {
    if (tag == null) {
      return metric;
    } else {
      return metric + "," + tag;
    }
  }
}
