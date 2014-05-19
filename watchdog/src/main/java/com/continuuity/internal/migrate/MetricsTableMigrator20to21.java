package com.continuuity.internal.migrate;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.data2.OperationException;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.AggregatesScanResult;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsTableFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 * Class for migrating reactor metrics table from reactor 2.0 to 2.1.  Some metrics bugs require some data migration to
 * occur to prevent backwards compatibility issues.  For example, "store.bytes" is written with a program context.  So
 * if you write to a dataset in a flowlet, it will have context app.f.flow.flowlet.  Then, if that app is deleted,
 * the values for 'store.bytes' in any context with that app will deleted.  Then, if somebody queries for
 * v2/metrics/reactor/store.bytes, that metric will have dropped which is misleading.  To fix this, we introduce
 * another metric 'dataset.store.bytes' that has a context of '-.dataset', which will be used for metrics that should
 * survive the deletion of application metrics.  In order to make sure the metric is correct, existing values for
 * 'store.bytes' need to be copied over to 'dataset.store.bytes', which is what this class does.
 */
public class MetricsTableMigrator20to21 implements TableMigrator {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableMigrator20to21.class);
  private final AggregatesTable aggregatesTable;

  @Inject
  public MetricsTableMigrator20to21(final MetricsTableFactory metricsTableFactory) {
    this.aggregatesTable = metricsTableFactory.createAggregates(MetricsScope.REACTOR.name());
  }

  @Override
  public void migrateIfRequired() throws OperationException {
    Set<String> metricsToCopy = getMetricsToCopy();
    if (metricsToCopy.size() > 0) {
      LOG.debug("Update of metrics table required.  Updating {}", metricsToCopy);
    }
    for (String metric : metricsToCopy) {
      copyAggregates(metric, "dataset." + metric, Constants.Metrics.DATASET_CONTEXT);
    }
  }

  /**
   * Copy the sum of values across all contexts for the original metric into a new metric in a new context.
   * For example, suppose the original metric is 'store.bytes' and it has a value of 10 in context
   * 'app1.f.flow1.flowlet2' and a value of 5 in context 'app2.f.flow2.flowlet5'.  If the new metric is
   * 'dataset.store.bytes' and the new context is '-.dataset', then it would have a value of 15 after the method
   * finishes.
   */
  private void copyAggregates(String originalMetric, String newMetric, String newContext) throws OperationException {
    AggregatesScanner scanner = aggregatesTable.scanAllTags(null, originalMetric);

    // runid is always "0" today, otherwise we'd need to keep track by runid as well.
    Map<String, Long> tagCounts = Maps.newHashMap();
    long untaggedCount = 0;

    // aggregate all the values in memory before writing to the aggregates table.  We could increment values as
    // we're scanning, but then a failure in the middle would cause ugly problems.
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      String metric = result.getMetric();
      // since scans are prefix match, it's possible we're looking to copy something like 'writes' but get back
      // a metric like 'write.failed'.
      if (!metric.equals(originalMetric)) {
        continue;
      }

      // gather tags and their values for the current (context, metric, runid) triple
      String tag = result.getTag();
      if (tag == null) {
        untaggedCount += result.getValue();
      } else {
        incrementCount(tagCounts, tag, result.getValue());
      }
    }

    aggregatesTable.swap(newContext, newMetric, "0", MetricsConstants.EMPTY_TAG, null, untaggedCount);
    for (Map.Entry<String, Long> tagEntry : tagCounts.entrySet()) {
      aggregatesTable.swap(newContext, newMetric, "0", tagEntry.getKey(), null, tagEntry.getValue());
    }
  }

  Set<String> getMetricsToCopy() {
    Set<String> metrics = Sets.newHashSet();
    // scan through all store metrics and look for the corresponding dataset metric in an application-less context.
    // in most cases, if one store metric has a corresponding dataset metric, all of them will.  But we check
    // each individual one in case the migration only partially completed previously due to some failure.
    AggregatesScanner scanner = aggregatesTable.scan(null, "store");
    while (scanner.hasNext()) {
      AggregatesScanResult result = scanner.next();
      String metric = result.getMetric();
      // if the corresponding dataset metric does not exist, we need to copy the metric.
      if (!aggregatesTable.scan(Constants.Metrics.DATASET_CONTEXT, "dataset." + metric).hasNext()) {
        metrics.add(metric);
      }
    }
    return metrics;
  }

  private void incrementCount(Map<String, Long> map, String key, long inc) {
    if (map.containsKey(key)) {
      map.put(key, map.get(key) + inc);
    } else {
      map.put(key, inc);
    }
  }
}
