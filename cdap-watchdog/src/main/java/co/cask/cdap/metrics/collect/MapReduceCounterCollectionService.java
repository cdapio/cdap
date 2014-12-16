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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * A {@link co.cask.cdap.metrics.collect.AggregatedMetricsCollectionService} that publish
 * {@link co.cask.cdap.metrics.transport.MetricsRecord} to MapReduce counters.
 */
@Singleton
public final class MapReduceCounterCollectionService extends AggregatedMetricsCollectionService {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceCounterCollectionService.class);
  private static final Pattern splitPattern = Pattern.compile("\\.");
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
  protected void publish(MetricsScope scope, Iterator<MetricsRecord> metrics) throws Exception {
    while (metrics.hasNext()) {
      MetricsRecord record = metrics.next();
      String context = record.getContext();
      boolean increment = false;

      // Context is expected to look like appId.b.programId.[m|r].[taskId]
      String counterGroup;
      String contextParts[] = splitPattern.split(context);
      //TODO: Refactor to support any context
      if (context.equals(Constants.Metrics.DATASET_CONTEXT)) {
        counterGroup = "cdap.dataset";
      } else if ("m".equals(contextParts[3])) {
        counterGroup = "cdap.mapper";
      } else if ("r".equals(contextParts[3])) {
        counterGroup = "cdap.reducer";
      } else {
        LOG.error("could not determine if the metric is a map or reduce metric from context {}, skipping...", context);
        continue;
      }

      counterGroup += "." + record.getType().toString();
      counterGroup += "." + scope.name();

      if (record.getType() == MetricType.COUNTER) {
        increment = true;
      }

      String counterName = getCounterName(record.getName());
      if (increment) {
        taskContext.getCounter(counterGroup, counterName).increment(record.getValue());
      } else {
        taskContext.getCounter(counterGroup, counterName).setValue(record.getValue());
      }

      for (TagMetric tag : record.getTags()) {
        counterName = getCounterName(record.getName(), tag.getTag());
        if (counterName != null) {
          if (increment) {
            taskContext.getCounter(counterGroup, counterName).increment(tag.getValue());
          } else {
            taskContext.getCounter(counterGroup, counterName).setValue(tag.getValue());
          }
        }
      }
    }
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
