/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.collect;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.transport.MetricsRecord;
import com.continuuity.metrics.transport.TagMetric;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * A {@link com.continuuity.metrics.collect.AggregatedMetricsCollectionService} that publish
 * {@link com.continuuity.metrics.transport.MetricsRecord} to MapReduce counters.
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

      // Context is expected to look like appId.b.programId.[m|r].[taskId]
      String counterGroup;
      String contextParts[] = splitPattern.split(context);
      //TODO: Refactor to support any context
      if (context.equals(Constants.Metrics.DATASET_CONTEXT)) {
        counterGroup = "continuuity.dataset";
      } else if ("m".equals(contextParts[3])) {
        counterGroup = "continuuity.mapper";
      } else if ("r".equals(contextParts[3])) {
        counterGroup = "continuuity.reducer";
      } else {
        LOG.error("could not determine if the metric is a map or reduce metric from context {}, skipping...", context);
        continue;
      }

      counterGroup += "." + scope.name();

      String counterName = getCounterName(record.getName());
      taskContext.getCounter(counterGroup, counterName).increment(record.getValue());
      for (TagMetric tag : record.getTags()) {
        counterName = getCounterName(record.getName(), tag.getTag());
        if (counterName != null) {
          taskContext.getCounter(counterGroup, counterName).increment(tag.getValue());
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
