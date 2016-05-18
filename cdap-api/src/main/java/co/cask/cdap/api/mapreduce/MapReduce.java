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

package co.cask.cdap.api.mapreduce;

import co.cask.cdap.api.ProgramLifecycle;

/**
 * Defines an interface for the MapReduce job. Use it for easy integration (re-use) of existing MapReduce jobs
 * that rely on the Hadoop MapReduce APIs.
 */
public interface MapReduce {

  /**
   * Configures a {@link MapReduce} job using the given {@link MapReduceConfigurer}.
   */
  void configure(MapReduceConfigurer configurer);

  /**
   * Invoked before starting a MapReduce job.
   * <p>
   * Users can access and modify the job configuration via {@link MapReduceContext#getHadoopJob()}, which returns
   * an instance of {@link <a href="http://hadoop.apache.org/docs/r2.3.0/api/org/apache/hadoop/mapreduce/Job.html">
   * org.apache.hadoop.mapreduce.Job</a>}.
   *
   * @param context job execution context
   * @throws Exception if there's an error during this method invocation
   * @deprecated Deprecated as of 3.5.0. Please use {@link ProgramLifecycle#initialize} instead, to initialize
   * the MapReduce program.
   */
  @Deprecated
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
   * @deprecated Deprecated as of 3.5.0. Please use {@link ProgramLifecycle#destroy} instead to execute the code once
   * MapReduce program is completed either successfully or on failure.
   */
  @Deprecated
  void onFinish(boolean succeeded, MapReduceContext context) throws Exception;
}
