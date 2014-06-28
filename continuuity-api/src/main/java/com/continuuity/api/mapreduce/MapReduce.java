package com.continuuity.api.mapreduce;

/**
 * Defines an interface for the MapReduce job. Use it for easy integration (re-use) of existing MapReduce jobs
 * that rely on the Hadoop MapReduce APIs.
 */
public interface MapReduce {
  /**
   * Configures a {@link MapReduce} job by returning a {@link MapReduceSpecification}.
   * @return An instance of {@link MapReduceSpecification}.
   */
  MapReduceSpecification configure();

  /**
   * Invoked before starting a MapReduce job.
   * <p>
   * Users can access and modify the job configuration via {@link MapReduceContext#getHadoopJob()}, which returns
   * an instance of {@link <a href="http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/Job.html">
   * org.apache.hadoop.mapreduce.Job</a>}.
   *
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  void beforeSubmit(MapReduceContext context) throws Exception;

  /**
   * Invoked after a MapReduce job finishes.
   * <p>
   *   Will not be called if: 
   *   <ul>
   *     <li>
   *       Job failed to start
   *     </li>
   *   </ul>
   * </p>
   * @param succeeded defines the result of job execution: true if job succeeded, false otherwise
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation.
   */
  void onFinish(boolean succeeded, MapReduceContext context) throws Exception;
}
