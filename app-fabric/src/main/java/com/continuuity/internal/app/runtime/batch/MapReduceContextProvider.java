package com.continuuity.internal.app.runtime.batch;

import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.batch.distributed.DistributedMapReduceContextBuilder;
import com.continuuity.internal.app.runtime.batch.inmemory.InMemoryMapReduceContextBuilder;
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

  public synchronized BasicMapReduceContext get() {
    if (context == null) {
      CConfiguration conf = contextConfig.getConf();
      context = getBuilder(conf)
        .build(conf,
               type,
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
