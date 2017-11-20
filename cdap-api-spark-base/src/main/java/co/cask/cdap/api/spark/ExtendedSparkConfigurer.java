/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.spark;

import co.cask.cdap.api.spark.dynamic.SparkCompiler;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;

import java.util.Arrays;

/**
 * Extends the functionality of {@link SparkConfigurer} to provide more spark specific feature at configuration time.
 */
public interface ExtendedSparkConfigurer extends SparkConfigurer {

  /**
   * Creates a new instance of {@link SparkCompiler} for Scala code compilation.
   *
   * @return a new instance of {@link SparkCompiler}.
   */
  SparkCompiler createSparkCompiler();

  /**
   * Adds a list of {@link SparkHttpServiceHandler}s to runs in the Spark driver.
   *
   * @see SparkHttpServiceHandler
   */
  default void addHandlers(SparkHttpServiceHandler...handlers) {
    addHandlers(Arrays.asList(handlers));
  }

  /**
   * Adds a list of {@link SparkHttpServiceHandler}s to runs in the Spark driver.
   *
   * @see SparkHttpServiceHandler
   */
  void addHandlers(Iterable<? extends SparkHttpServiceHandler> handlers);
}
