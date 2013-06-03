package com.continuuity.test.app;

import com.continuuity.api.data.DataSet;

/**
 * Instance of this class is for managing deployed application.
 *
 */
public interface ApplicationManager {

  /**
   * Starts a flow.
   * @param flowName Name of the flow to start.
   * @return A {@link FlowManager} for controlling the started flow.
   */
  FlowManager startFlow(String flowName);

  /**
   * Starts a mapreduce job.
   * @param jobName Name of the mapreduce job to start.
   * @return A {@link MapReduceManager} for controlling the started mapreduce job.
   */
  MapReduceManager startMapReduce(String jobName);

  /**
   * Starts a procedure.
   * @param procedureName Name of the procedure to start.
   * @return A {@link ProcedureManager} for controlling the started procedure.
   */
  ProcedureManager startProcedure(String procedureName);

  /**
   * Gets a {@link StreamWriter} for writing data to the given stream.
   * @param streamName Name of the stream to write to.
   * @return A {@link StreamWriter}.
   */
  StreamWriter getStreamWriter(String streamName);

  /**
   * Gets an instance of {@link com.continuuity.api.data.DataSet} of the given dataset name.
   * Operations on the returned {@link com.continuuity.api.data.DataSet} always performed synchronously,
   * i.e. no support for multi-operations transaction.
   * @param dataSetName Name of the dataset to retrieve.
   * @param <T> Type of the dataset.
   * @return A {@link com.continuuity.api.data.DataSet} instance of the given dataset type.
   */
  <T extends DataSet> T getDataSet(String dataSetName);

  /**
   * Stops all processors managed by this manager and clear all associated runtime metrics.
   */
  void stopAll();
}
