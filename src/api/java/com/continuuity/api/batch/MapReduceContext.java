package com.continuuity.api.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;

import java.util.List;

/**
 * Mapreduce job execution context.
 */
public interface MapReduceContext {
  /**
   * Given a name of dataset, returns an instance of {@link com.continuuity.api.data.DataSet}.
   * @param name of the {@link com.continuuity.api.data.DataSet}.
   * @param <T> The specific {@link com.continuuity.api.data.DataSet} type requested.
   * @return An instance of {@link com.continuuity.api.data.DataSet}.
   */
  <T extends DataSet> T getDataSet(String name);

  /**
   * @return The specification used to configure this {@link MapReduce} instance.
   */
  MapReduceSpecification getSpecification();

  /**
   * @return an instance of {@link org.apache.hadoop.mapreduce.Job} that used to submit the job to Hadoop cluster.
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
