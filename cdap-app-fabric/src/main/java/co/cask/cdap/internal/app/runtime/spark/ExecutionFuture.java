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

import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * A {@link ListenableFuture} that represents execution of a job in the Spark framework.
 *
 * @param <V> Type of the future result
 * @see SparkSubmitter#submit(ExecutionSparkContext, Map, List, File, Object)
 */
public interface ExecutionFuture<V> extends ListenableFuture<V> {

  /**
   * Returns the {@link ExecutionSparkContext} representing the Spark program being executed.
   */
  ExecutionSparkContext getSparkContext();
}
