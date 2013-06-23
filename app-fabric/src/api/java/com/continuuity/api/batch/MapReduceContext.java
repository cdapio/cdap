package com.continuuity.api.batch;

import com.continuuity.api.RuntimeContext;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;

import java.util.List;

/**
 * Mapreduce job execution context.
 */
public interface MapReduceContext extends RuntimeContext {
  /**
   * @return The specification used to configure this {@link MapReduce} instance.
   */
  MapReduceSpecification getSpecification();

  /**
   */
  <T> T getHadoopJob();

  /**
   * Overrides input configuration of this mapreduce job to use given dataset and given data selection splits.
   * @param dataset input dataset
   * @param splits data selection splits
   */
  void setInput(BatchReadable dataset, List<Split> splits);

  /**
   * Overrides output configuration of this mapreduce job to write to given dataset
   * @param dataset output dataset
   */
  void setOutput(BatchWritable dataset);
}
