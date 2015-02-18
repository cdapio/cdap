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

import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Iterator;
import java.util.Map;

/**
 * A {@link co.cask.cdap.metrics.collect.AggregatedMetricsCollectionService} that publish
 * {@link MetricValue} to MapReduce counters.
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
  protected void publish(Iterator<MetricValue> metrics) throws Exception {
    while (metrics.hasNext()) {
      MetricValue record = metrics.next();

      // The format of the counters:
      // * counter group name: "cdap.<tag_name>.<tag_value>[.<tag_name>.<tag_value>[...]]
      // * counter name: metric name

      StringBuilder counterGroup = new StringBuilder("cdap");
      // flatten tags
      for (Map.Entry<String, String> tag : record.getTags().entrySet()) {
        counterGroup.append(".").append(tag.getKey()).append(".").append(tag.getValue());
      }

      String counterName = getCounterName(record.getName());
      if (record.getType() == MetricType.COUNTER) {
        taskContext.getCounter(counterGroup.toString(), counterName).increment(record.getValue());
      } else {
        taskContext.getCounter(counterGroup.toString(), counterName).setValue(record.getValue());
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
      tags.put(tagName, tagValue);
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
