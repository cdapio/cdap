/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.environment;

import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The interface is the integration point for CDAP master runtime provider.
 */
public interface MasterEnvironment {

  /**
   * This method will be invoked to initialize this {@link MasterEnvironment}.
   * It will be called before any other method is called.
   *
   * @param context a {@link MasterEnvironmentContext} to provide information about the CDAP environment
   * @throws Exception if initialization failed
   */
  default void initialize(MasterEnvironmentContext context) throws Exception {
    // no-op by default
  }

  /**
   * This method will be invoked to destroy this {@link MasterEnvironment}.
   * This will be the last method called on this instance.
   */
  default void destroy() {
    // no-op by default
  }

  /**
   * Returns a {@link Optional} {@link MasterEnvironmentTask} to be executed periodically.
   * It is guaranteed that there is no concurrent call to the task returned.
   */
  default Optional<MasterEnvironmentTask> getTask() {
    return Optional.empty();
  }

  /**
   * Creates a new instance of {@link MasterEnvironmentRunnable} from the given class name.
   *
   * @param context a {@link MasterEnvironmentRunnableContext} to provide access to CDAP resources.
   * @param runnableClass the {@link MasterEnvironmentRunnable} class to create an instance from
   * @return a new instance of the given class
   * @throws Exception if failed to create a new instance
   */
  MasterEnvironmentRunnable createRunnable(MasterEnvironmentRunnableContext context,
                                           Class<? extends MasterEnvironmentRunnable> runnableClass) throws Exception;

  /**
   * Returns the name of this environment implementation.
   */
  String getName();

  /**
   * Returns a {@link Supplier} of {@link DiscoveryService} for service announcement purpose.
   */
  Supplier<DiscoveryService> getDiscoveryServiceSupplier();

  /**
   * Returns a {@link Supplier} of {@link DiscoveryServiceClient} for service discovery purpose.
   */
  Supplier<DiscoveryServiceClient> getDiscoveryServiceClientSupplier();

  /**
   * Returns a {@link Supplier} of {@link TwillRunnerService} for running programs.
   */
  Supplier<TwillRunnerService> getTwillRunnerSupplier();

  /**
   * Spark confs.
   */
  default SparkConfigs getSparkConf() {
    throw new RuntimeException("Method not supported");
  }
}
