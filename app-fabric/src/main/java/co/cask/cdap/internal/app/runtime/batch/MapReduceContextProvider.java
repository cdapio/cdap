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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import co.cask.cdap.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides access to MapReduceContext for mapreduce job tasks.
 */
public final class MapReduceContextProvider {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceContextProvider.class);

  private final TaskAttemptContext taskContext;
  private final MapReduceMetrics.TaskType type;
  private final MapReduceContextConfig contextConfig;
  private BasicMapReduceContext context;
  private AbstractMapReduceContextBuilder contextBuilder;

  public MapReduceContextProvider(TaskAttemptContext context) {
    this(context, null);
  }

  public MapReduceContextProvider(TaskAttemptContext context, MapReduceMetrics.TaskType type) {
    this.taskContext = context;
    this.type = type;
    this.contextConfig = new MapReduceContextConfig(context);
    this.contextBuilder = null;
  }

  /**
   * Creates an instance of {@link BasicMapReduceContext} that the {@link co.cask.cdap.app.program.Program} contained
   * inside cannot load program classes. It is used for the cases where only the application specification is needed,
   * but no need to load any class from it.
   */
  public synchronized BasicMapReduceContext get() {
    if (context == null) {
      CConfiguration conf = contextConfig.getConf();
      context = getBuilder(conf)
        .build(type,
               contextConfig.getRunId(),
               contextConfig.getLogicalStartTime(),
               contextConfig.getWorkflowBatch(),
               contextConfig.getArguments(),
               contextConfig.getTx(),
               taskContext.getConfiguration().getClassLoader(),
               contextConfig.getProgramLocation(),
               contextConfig.getInputDataSet(),
               contextConfig.getInputSelection(),
               contextConfig.getOutputDataSet());
    }
    return context;
  }

  private synchronized AbstractMapReduceContextBuilder getBuilder(CConfiguration conf) {
    if (contextBuilder == null) {
      String mrFramework = taskContext.getConfiguration().get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
      if ("local".equals(mrFramework)) {
        contextBuilder = new InMemoryMapReduceContextBuilder(conf, taskContext);
      } else {
        // mrFramework = "yarn" or "classic"
        // if the jobContext is not a TaskAttemptContext, mrFramework should not be yarn.
        contextBuilder = new DistributedMapReduceContextBuilder(
          conf, HBaseConfiguration.create(taskContext.getConfiguration()), taskContext);
      }
    }
    return contextBuilder;
  }

}
