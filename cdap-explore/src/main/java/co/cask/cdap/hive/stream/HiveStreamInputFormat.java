/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.hive.stream;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamInputSplitFactory;
import co.cask.cdap.data.stream.StreamInputSplitFinder;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.explore.HiveUtilities;
import co.cask.cdap.hive.context.ContextManager;
import co.cask.cdap.proto.id.StreamId;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Stream input format for use in hive queries and only hive queries. Will not work outside of hive.
 */
public class HiveStreamInputFormat implements InputFormat<Void, ObjectWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    // right before this method is called by hive, hive copies everything that the storage handler put in the properties
    // map in it's configureTableJobProperties method into the job conf. We put the stream name in there so that
    // we can derive the stream path and properties from it.
    // this MUST be done in the input format and not in StreamSerDe's initialize method because we have no control
    // over when initialize is called. If we set job conf settings there, the settings for one stream get clobbered
    // by the settings for another stream if a join over streams is being performed.
    StreamInputSplitFinder<InputSplit> splitFinder = getSplitFinder(conf);
    List<InputSplit> splits = splitFinder.getSplits(conf);
    return splits.toArray(new InputSplit[splits.size()]);
  }

  @Override
  public RecordReader<Void, ObjectWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    return new StreamRecordReader(split, conf);
  }

  public static StreamId getStreamId(JobConf conf) {
    String streamName = conf.get(Constants.Explore.STREAM_NAME);
    String streamNamespace = conf.get(Constants.Explore.STREAM_NAMESPACE);
    return new StreamId(streamNamespace, streamName);
  }

  private StreamInputSplitFinder<InputSplit> getSplitFinder(JobConf conf) throws IOException {
    // first get the context we are in
    ContextManager.Context context = ContextManager.getContext(conf);
    Preconditions.checkNotNull(context);
    StreamConfig streamConfig = context.getStreamConfig(getStreamId(conf));
    // make sure we get the current generation so we don't read events that occurred before a truncate.
    Location streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation(),
                                                               StreamUtils.getGeneration(streamConfig));

    StreamInputSplitFinder.Builder builder = StreamInputSplitFinder.builder(streamPath.toURI());

    // Get the Hive table path for the InputSplit created. It is just to satisfy hive. The InputFormat never uses it.
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(Job.getInstance(conf));
    final Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);

    return setupBuilder(conf, streamConfig, builder).build(new StreamInputSplitFactory<InputSplit>() {
      @Override
      public InputSplit createSplit(Path eventPath, Path indexPath, long startTime, long endTime,
                                    long start, long length, @Nullable String[] locations) {
        return new StreamInputSplit(tablePaths[0], eventPath, indexPath, startTime, endTime, start, length, locations);
      }
    });
  }

  /**
   * Setups the given {@link StreamInputSplitFinder.Builder} by analyzing the query.
   */
  private StreamInputSplitFinder.Builder setupBuilder(Configuration conf, StreamConfig streamConfig,
                                                      StreamInputSplitFinder.Builder builder) {
    // the conf contains a 'hive.io.filter.expr.serialized' key which contains the serialized form of ExprNodeDesc
    long startTime = Math.max(0L, System.currentTimeMillis() - streamConfig.getTTL());
    long endTime = System.currentTimeMillis();

    String serializedExpr = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (serializedExpr == null) {
      return builder.setStartTime(startTime).setEndTime(endTime);
    }

    try {
      ExprNodeGenericFuncDesc expr = HiveUtilities.deserializeExpression(serializedExpr, conf);

      // Analyze the query to extract predicates that can be used for indexing (i.e. setting start/end time)
      IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
      for (CompareOp op : CompareOp.values()) {
        analyzer.addComparisonOp(op.getOpClassName());
      }

      // Stream can only be indexed by timestamp
      analyzer.clearAllowedColumnNames();
      analyzer.allowColumnName("ts");

      List<IndexSearchCondition> conditions = Lists.newArrayList();
      analyzer.analyzePredicate(expr, conditions);

      for (IndexSearchCondition condition : conditions) {
        CompareOp op = CompareOp.from(condition.getComparisonOp());
        if (op == null) {
          // Not a supported operation
          continue;
        }
        ExprNodeConstantDesc value = condition.getConstantDesc();
        if (value == null || !(value.getValue() instanceof Long)) {
          // Not a supported value
          continue;
        }

        long timestamp = (Long) value.getValue();
        // If there is a equal, set both start and endtime and no need to inspect further
        if (op == CompareOp.EQUAL) {
          startTime = timestamp;
          endTime = (timestamp < Long.MAX_VALUE) ? timestamp + 1L : timestamp;
          break;
        }
        if (op == CompareOp.GREATER || op == CompareOp.EQUAL_OR_GREATER) {
          // Plus 1 for the start time if it is greater since start time is inclusive in stream
          startTime = Math.max(startTime,
                               timestamp + (timestamp < Long.MAX_VALUE && op == CompareOp.GREATER ? 1L : 0L));
        } else {
          // Plus 1 for end time if it is equal or less since end time is exclusive in stream
          endTime = Math.min(endTime,
                             timestamp + (timestamp < Long.MAX_VALUE && op == CompareOp.EQUAL_OR_LESS ? 1L : 0L));
        }
      }
    } catch (Throwable t) {
      LOG.warn("Exception analyzing query predicate. A full table scan will be performed.", t);
    }

    return builder.setStartTime(startTime).setEndTime(endTime);
  }

  private enum CompareOp {
    EQUAL(GenericUDFOPEqual.class.getName()),
    EQUAL_OR_GREATER(GenericUDFOPEqualOrGreaterThan.class.getName()),
    EQUAL_OR_LESS(GenericUDFOPEqualOrLessThan.class.getName()),
    GREATER(GenericUDFOPGreaterThan.class.getName()),
    LESS(GenericUDFOPLessThan.class.getName());

    private final String opClassName;

    CompareOp(String opClassName) {
      this.opClassName = opClassName;
    }

    public String getOpClassName() {
      return opClassName;
    }

    /**
     * Returns a {@link CompareOp} by matching the given class name or {@code null} if there is none matching.
     */
    @Nullable
    public static CompareOp from(String opClassName) {
      for (CompareOp op : values()) {
        if (op.getOpClassName().equals(opClassName)) {
          return op;
        }
      }
      return null;
    }
  }
}
