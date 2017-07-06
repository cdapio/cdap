/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.spark.SparkClientContext;
import co.cask.cdap.etl.api.batch.BatchAggregatorContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.DatasetContextLookupProvider;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.hadoop.mapreduce.Job;

/**
 * Batch Aggregator Context.
 */
public class DefaultAggregatorContext extends AbstractBatchContext implements BatchAggregatorContext {
  private final Job job;
  private Integer numPartitions;
  private Class<?> groupKeyClass;
  private Class<?> groupValueClass;

  public DefaultAggregatorContext(MapReduceContext context, Metrics metrics, StageSpec stageSpec) {
    super(context, metrics, new DatasetContextLookupProvider(context), context.getLogicalStartTime(),
          context.getAdmin(), stageSpec, new BasicArguments(context));
    this.job = context.getHadoopJob();
  }

  public DefaultAggregatorContext(SparkClientContext context, StageSpec stageSpec) {
    super(context, context.getMetrics(), new DatasetContextLookupProvider(context), context.getLogicalStartTime(),
          context.getAdmin(), stageSpec, new BasicArguments(context));
    this.job = null;
  }

  @Override
  public void setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException(String.format(
        "Invalid value for numPartitions %d. It must be a positive integer.", numPartitions));
    }
    this.numPartitions = numPartitions;
  }

  @Override
  public void setGroupKeyClass(Class<?> groupKeyClass) {
    this.groupKeyClass = groupKeyClass;
  }

  @Override
  public void setGroupValueClass(Class<?> groupValueClass) {
    this.groupValueClass = groupValueClass;
  }

  public Integer getNumPartitions() {
    return numPartitions;
  }

  public Class<?> getGroupKeyClass() {
    return groupKeyClass;
  }

  public Class<?> getGroupValueClass() {
    return groupValueClass;
  }

  @Override
  public <T> T getHadoopJob() {
    if (job == null) {
      throw new UnsupportedOperationException("Hadoop Job is not available in Spark");
    }
    return (T) job;
  }
}
