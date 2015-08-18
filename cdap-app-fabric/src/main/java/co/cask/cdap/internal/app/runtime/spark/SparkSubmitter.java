/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.spark.SparkProgram;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Interface to provide abstraction for submitting a {@link SparkProgram} to
 * the Spark framework for execution.
 */
public interface SparkSubmitter {

  /**
   * Submits a Spark job to the Spark framework.
   *
   * @param sparkContext the {@link ExecutionSparkContext} representing the Spark program
   * @param configs configurations for the Spark framework
   * @param resources list of resources to be localized to Spark containers
   * @param jobJar location of the job JAR file required by the framework
   * @param result object instance to be available through the returned {@link ExecutionFuture} when it completes
   * @param <V> Type of the result object
   * @return An {@link ExecutionFuture} that will be completed when the job finished. If the job execution failed,
   *         the future will also be failed with the cause wrapped inside an {@link ExecutionException}
   *         when {@link ExecutionFuture#get} is called. If {@link ExecutionFuture#cancel(boolean)} is called,
   *         the running job will be terminated immediately.
   */
  <V> ExecutionFuture<V> submit(ExecutionSparkContext sparkContext, Map<String, String> configs,
                                List<LocalizeResource> resources, File jobJar, V result);
}
