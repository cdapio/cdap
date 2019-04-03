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

import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;

import java.util.Arrays;

/**
 * This abstract class provides extra features at configure time to {@link Spark} program through the
 * {@link ExtendedSparkConfigurer} provided by the {@link #getConfigurer()} method.
 */
public abstract class AbstractExtendedSpark extends AbstractSpark {

  /**
   * Returns the {@link ExtendedSparkConfigurer} for extended features, only available at configuration time.
   */
  @Override
  protected ExtendedSparkConfigurer getConfigurer() {
    SparkConfigurer configurer = super.getConfigurer();
    if (!(configurer instanceof ExtendedSparkConfigurer)) {
      // This shouldn't happen, unless there is bug in app-fabric
      throw new IllegalStateException(
        "Expected the configurer is an instance of " + ExtendedSparkConfigurer.class.getName() +
          ", but get " + configurer.getClass().getName() + " instead.");
    }
    return (ExtendedSparkConfigurer) configurer;
  }

  /**
   * Adds a list of {@link SparkHttpServiceHandler}s to runs in the Spark driver.
   */
  protected void addHandlers(SparkHttpServiceHandler...handlers) {
    addHandlers(Arrays.asList(handlers));
  }

  /**
   * Adds a list of {@link SparkHttpServiceHandler}s to runs in the Spark driver.
   */
  protected void addHandlers(Iterable<? extends SparkHttpServiceHandler> handlers) {
    getConfigurer().addHandlers(handlers);
  }
}
