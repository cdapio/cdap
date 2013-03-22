package com.continuuity.api.batch.hadoop;

/**
 * Defines an interface for the mapreduce job. Use it for easy integration (re-use) of the existing mapreduce jobs
 * that rely on Hadoop MapReduce APIs
 */
public interface HadoopMapReduceJob {
  /**
   * Configures {@link HadoopMapReduceJob} by returning a {@link HadoopMapReduceJobSpecification}
   * @return an instance of {@link HadoopMapReduceJobSpecification}
   */
  HadoopMapReduceJobSpecification configure();

  /**
   * Invoked before starting mapreduce job.
   * <p>
   * User can access and modify job configuration via {@link HadoopMapReduceJobContext#getHadoopJobConf()} which returns
   * an instance of {@link org.apache.hadoop.mapreduce.Job}.
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  void beforeSubmit(HadoopMapReduceJobContext context) throws Exception;

  /**
   * Invoked after mapreduce job finishes.
   * <p>
   *   Will not be called in the following cases:
   *   <ul>
   *     <li>
   *       Job failed to start
   *     </li>
   *     <li>
   *       App Fabric stopped watching for the job completion (for whatever reason, like restart of AppFabric)
   *     </li>
   *   </ul>
   * </p>
   * @param succeeded defines the result of job execution: true if job succeeded, false otherwise
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   */
  void onFinish(boolean succeeded, HadoopMapReduceJobContext context) throws Exception;
}
