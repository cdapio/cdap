/*
 * Copyright 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.process;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import co.cask.cdap.metrics.transport.MetricsRecord;
import co.cask.cdap.metrics.transport.TagMetric;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/**
 * Builds {@link MetricsRecord}s out of {@link MetricValue}s to be stored to serve metric queries
 * (to support CDAP built-in metrics system REST API).
 * <p/>
 *
 * It uses "raw" emitted metric value to build {@link MetricsRecord}s to store in the metrics persistence. These
 * {@link MetricsRecord}s a built based on how metrics data is going to be queried. You can think of them as
 * pre-aggregations persisted for fast querying.
 *
 * The {@link MetricsRecord}s are built based on defined {@link #AGGREGATE_RULES}.
 */
public class MetricRecordsWrapper implements Iterator<MetricsRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricRecordsWrapper.class);

  // Rules by which to build MetricsRecord aggregations.
  // Note that one rule may be fully "contain" another rule in which case we want only the former to produce
  // MetricsRecord and the letter one to be skipped when building MetricsRecord. We say rule A "contains" rule B
  // if first tags of rule A are exactly the same as all the tags of rule B.
  // The way metrics query works currently, if we persist MetricsRecords built from both rules, we would
  // be double counting at query time.
  private static final List<Rule> AGGREGATE_RULES;

  static {
    // For better readability we define rules from shorter to more detailed. The current fanout logic
    // however requires the reverse order, to avoid emitting duplicate metrics records. todo: define duplicate
    // So, in the end we reverse the list;
    List<Rule> rules = Lists.newLinkedList();
    // <cluster metrics>, e.g. storage used
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.CLUSTER_METRICS)));
    // namespace
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE)));
    // namespace, app, prg type, prg name
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM)));
    // namespace, app, prg type, prg name, instance id
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.INSTANCE_ID)));
    // namespace, app, prg type, prg name, flowlet name, tag: queue name (for flowlet only)
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.FLOWLET),
                       Constants.Metrics.Tag.FLOWLET_QUEUE));
    // namespace, app, prg type, prg name, flowlet name, instance id (for flowlet only)
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.FLOWLET,
                                        Constants.Metrics.Tag.INSTANCE_ID)));
    // namespace, app, prg type, prg name, mr task type (for mr task only) - map and reduce report progress overall
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.MR_TASK_TYPE)));
    // namespace, app, prg type, prg name, mr task type, task id (for mr task only)
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.MR_TASK_TYPE, Constants.Metrics.Tag.INSTANCE_ID)));
    // namespace, app, prg type, prg name, service runnable (for service only)
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
                                        Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM,
                                        Constants.Metrics.Tag.SERVICE_RUNNABLE)));
    // component
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.COMPONENT)));
    // component, handler
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER)));
    // component, handler, method
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER,
                                        Constants.Metrics.Tag.METHOD)));
    // component, handler, instance id
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER,
                                        Constants.Metrics.Tag.INSTANCE_ID)));
    // component, handler, instance id, tag: stream
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER,
                                        Constants.Metrics.Tag.INSTANCE_ID),
                       Constants.Metrics.Tag.STREAM));
    // dataset name
    // note: weird rule, but this is what we had before
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.DATASET), Constants.Metrics.Tag.DATASET));

    // service, handler, method
    rules.add(new Rule(ImmutableList.of(Constants.Metrics.Tag.SERVICE,
                                        Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD)));

    Collections.reverse(rules);

    AGGREGATE_RULES = ImmutableList.copyOf(rules);
  }

  private final Iterator<MetricValue> rawIterator;
  private Iterator<MetricsRecord> current;

  public MetricRecordsWrapper(Iterator<MetricValue> rawIterator) {
    this.rawIterator = rawIterator;
  }


  @Override
  public boolean hasNext() {
    if (current != null && current.hasNext()) {
      return true;
    }
    List<MetricsRecord> fanout = null;
    while (fanout == null || fanout.isEmpty()) {
      if (!rawIterator.hasNext()) {
        return false;
      }

      fanout = getAggregations(rawIterator.next());
    }

    current = fanout.iterator();
    return true;
  }

  @Override
  public MetricsRecord next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return current.next();
  }

  private List<MetricsRecord> getAggregations(MetricValue metricValue) {
    // We go thru all aggregate rules and build MetricsRecords for the given metricValue.
    // All tags defined by the rule must be present in the given metricValue for MetricsRecord to be built. Otherwise
    // the rule is skipped.
    // Also, we skip metric rule if it is fully "contained" in the other metric rule that already produced MetricRecord
    // to avoid double aggregation. See AGGREGATE_RULES comments for more info.

    List<MetricsRecord> result = Lists.newLinkedList();
    List<Rule> rulesUsed = Lists.newLinkedList();

    for (Rule rule : AGGREGATE_RULES) {
      if (!contains(rulesUsed, rule)) {
        MetricsRecord record = getMetricsRecord(metricValue, rule);
        if (record != null) {
          result.add(record);
          rulesUsed.add(rule);
        }
      }
    }

    return result;
  }

  private boolean contains(List<Rule> rules, Rule rule) {
    for (Rule candidate : rules) {
      return contains(candidate, rule);
    }
    return false;
  }

  private boolean contains(Rule candidate, Rule rule) {
    // verifies if candidate contains all tags of the given rule and they are in the same order

    Iterator<String> tagsToCompare = candidate.tagsToPutIntoContext.iterator();
    for (String next : rule.tagsToPutIntoContext) {
      boolean found = false;
      while (tagsToCompare.hasNext()) {
        if (next.equals(tagsToCompare.next())) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private MetricsRecord getMetricsRecord(MetricValue metricValue, Rule rule) {
    String runId = metricValue.getTags().get(Constants.Metrics.Tag.RUN_ID);
    runId = runId == null ? "0" : runId;
    MetricsRecordBuilder builder = new MetricsRecordBuilder(runId, metricValue.getName(), metricValue.getTimestamp(),
                                                            metricValue.getValue(), metricValue.getType());

    int index = 0;
    for (String tagName : rule.tagsToPutIntoContext) {
      String tagValue = metricValue.getTags().get(tagName);
      if (tagValue != null) {
        addToContext(builder, tagName, tagValue, index);
      } else {
        // doesn't fit the rule: some tag is missing
        return null;
      }
    }

    for (String tagName : rule.tagsToPutIntoTags) {
      String tagValue = metricValue.getTags().get(tagName);
      if (tagValue != null) {
        builder.addTag(tagValue);
      }
    }

    // todo: move to rules?
    // adjusting metric name based on emitter
    String scope = metricValue.getTags().get(Constants.Metrics.Tag.SCOPE);
    // by default, metric is emitted by the framework
    builder.prefixMetricName(scope == null ? "system." : scope + ".");

    return builder.build();
  }

  private void addToContext(MetricsRecordBuilder builder, String tagName, String tagValue, int index) {
    if (Constants.Metrics.Tag.DATASET.equals(tagName) && index == 0) {
      // special handling for dataset context metrics - legacy
      builder.appendContext("-.dataset");
    } else if (Constants.Metrics.Tag.CLUSTER_METRICS.equals(tagName) && index == 0) {
      // special handling for cluster metrics - legacy
      builder.appendContext("-.cluster");
    } else {
      builder.appendContext(tagValue);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove operation is not supported");
  }

  private static final class MetricsRecordBuilder {
    private final StringBuilder context;      // Program context of where the metric get generated.
    private final String runId;               // RunId
    private final StringBuilder name;         // Name of the metric
    private final List<TagMetric> tags;       // List of TagMetric
    private final long timestamp;             // Timestamp in second of when the metric happened.
    private final long value;                 // Value of the metric, regardless of tags
    private final MetricType type;            // Type of the metric value, to be set(gauge) or to increment

    public MetricsRecordBuilder(String runId, String name, long timestamp, long value, MetricType type) {
      this.runId = runId;
      this.name = new StringBuilder(name);
      this.timestamp = timestamp;
      this.value = value;
      this.type = type;
      this.context = new StringBuilder();
      this.tags = new ArrayList<TagMetric>();
    }

    public void prefixMetricName(String prefix) {
      name.insert(0, prefix);
    }

    public void appendContext(String contextPart) {
      context.append(contextPart).append(".");
    }

    public void addTag(String tagValue) {
      // adding same: todo: explain
      tags.add(new TagMetric(tagValue, value));
    }

    public MetricsRecord build() {
      if (context.length() == 0) {
        return null;
      }
      // delete last "."
      context.deleteCharAt(context.length() - 1);
      return new MetricsRecord(context.toString(), runId, name.toString(), tags, timestamp, value, type);
    }
  }

  private static final class Rule {
    private final List<String> tagsToPutIntoContext;
    private final List<String> tagsToPutIntoTags;

    private Rule(List<String> tagsToPutIntoContext) {
      this(tagsToPutIntoContext, Collections.<String>emptyList());
    }

    private Rule(List<String> tagsToPutIntoContext, String tagToPutIntoTags) {
      this(tagsToPutIntoContext, ImmutableList.of(tagToPutIntoTags));
    }

    private Rule(List<String> tagsToPutIntoContext, List<String> tagsToPutIntoTags) {
      this.tagsToPutIntoContext = tagsToPutIntoContext;
      this.tagsToPutIntoTags = tagsToPutIntoTags;
    }
  }
}
