package com.continuuity.api.mapreduce;

/**
 * This abstract class provides a default implementation of {@link MapReduce} methods for easy extension.
 */
public abstract class AbstractMapReduce implements MapReduce {
  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    // Do nothing by default
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    // Do nothing by default
  }
}
