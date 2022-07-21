/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.util.concurrent.ListenableFuture;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Interface to provide abstraction for submitting a Spark program to
 * the Spark framework for execution.
 */
public interface SparkSubmitter {

  /**
   * Submits a Spark job to the Spark framework.
   *
   * @param runtimeContext the {@link SparkRuntimeContext} representing the Spark program
   * @param configs configurations for the Spark framework
   * @param resources list of resources to be localized to Spark containers
   * @param jobFile location of the job file required by the framework
   * @param result object instance to be available through the returned {@link ListenableFuture} when it completes
   * @param <V> Type of the result object
   * @return An {@link SparkJobFuture} that will be completed when the job finished. If the job execution failed,
   *         the future will also be failed with the cause wrapped inside an {@link ExecutionException}
   *         when {@link SparkJobFuture#get} is called. If {@link SparkJobFuture#cancel(boolean)} is called,
   *         the running job will be terminated immediately. To terminate the job gracefully with a timeout, use
   *         {@link SparkJobFuture#cancel(long, TimeUnit)}
   * @throws Exception if there is error while submitting the spark job
   */
  <V> SparkJobFuture<V> submit(SparkRuntimeContext runtimeContext, Map<String, String> configs,
                                 List<LocalizeResource> resources, URI jobFile, V result) throws Exception;
}
