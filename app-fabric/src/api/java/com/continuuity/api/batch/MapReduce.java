package com.continuuity.api.batch;

/**
 * Defines an interface for the mapreduce job. Use it for easy integration (re-use) of the existing mapreduce jobs
 * that rely on Hadoop MapReduce APIs
 */
public interface MapReduce {
  /**
   * Configures {@link MapReduce} by returning a {@link MapReduceSpecification}.
   * @return an instance of {@link MapReduceSpecification}
   */
  MapReduceSpecification configure();

  /**
   * Invoked before starting mapreduce job.
   * <p>
   * User can access and modify job configuration via {@link MapReduceContext#getHadoopJob()} which returns
   * an instance of {@link org.apache.hadoop.mapreduce.Job}.
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  void beforeSubmit(MapReduceContext context) throws Exception;

  /**
   * Invoked after mapreduce job finishes.
   * <p>
   *   Will not be called in the following cases:
   *   <ul>
   *     <li>
   *       Job failed to start
   *     </li>
   *   </ul>
   * </p>
   * @param succeeded defines the result of job execution: true if job succeeded, false otherwise
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  void onFinish(boolean succeeded, MapReduceContext context) throws Exception;
}
