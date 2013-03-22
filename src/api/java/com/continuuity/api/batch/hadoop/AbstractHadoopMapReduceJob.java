package com.continuuity.api.batch.hadoop;

/**
 * This abstract class provides default implementation of {@link HadoopMapReduceJob} methods for easy extension.
 */
public abstract class AbstractHadoopMapReduceJob implements HadoopMapReduceJob {
  @Override
  public void beforeSubmit(HadoopMapReduceJobContext context) throws Exception {
    // Do nothing by default
  }

  @Override
  public void onFinish(boolean succeeded, HadoopMapReduceJobContext context) throws Exception {
    // Do nothing by default
  }
}
