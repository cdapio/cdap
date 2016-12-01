/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.common;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.master.startup.ExploreServiceResourceKeys;
import co.cask.cdap.master.startup.ServiceResourceKeys;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Provides utility methods for CDAP master checks
 */
public final class MasterUtils {

  /**
   * Explicitly disallow default constructor for utility class
   */
  private MasterUtils(){}

  /**
   * Creates a set of system service resource keys
   * @param cConf the configured CDAP settings
   * @return a set of {@link ServiceResourceKeys} for all system services
   */
  public static Set<ServiceResourceKeys> createSystemServicesResourceKeysSet(CConfiguration cConf) {
    ImmutableSet.Builder<ServiceResourceKeys> builder = ImmutableSet.<ServiceResourceKeys>builder()
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.TRANSACTION,
                                   Constants.Transaction.Container.MEMORY_MB,
                                   Constants.Transaction.Container.NUM_CORES,
                                   Constants.Transaction.Container.NUM_INSTANCES,
                                   Constants.Transaction.Container.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.STREAMS,
                                   Constants.Stream.CONTAINER_MEMORY_MB,
                                   Constants.Stream.CONTAINER_VIRTUAL_CORES,
                                   Constants.Stream.CONTAINER_INSTANCES,
                                   Constants.Stream.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.METRICS,
                                   Constants.Metrics.MEMORY_MB,
                                   Constants.Metrics.NUM_CORES,
                                   Constants.Metrics.NUM_INSTANCES,
                                   Constants.Metrics.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.METRICS_PROCESSOR,
                                   Constants.MetricsProcessor.MEMORY_MB,
                                   Constants.MetricsProcessor.NUM_CORES,
                                   Constants.MetricsProcessor.NUM_INSTANCES,
                                   Constants.MetricsProcessor.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.LOGSAVER,
                                   Constants.LogSaver.MEMORY_MB,
                                   Constants.LogSaver.NUM_CORES,
                                   Constants.LogSaver.NUM_INSTANCES,
                                   Constants.LogSaver.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.DATASET_EXECUTOR,
                                   Constants.Dataset.Executor.CONTAINER_MEMORY_MB,
                                   Constants.Dataset.Executor.CONTAINER_VIRTUAL_CORES,
                                   Constants.Dataset.Executor.CONTAINER_INSTANCES,
                                   Constants.Dataset.Executor.MAX_INSTANCES))
      .add(new ServiceResourceKeys(cConf,
                                   Constants.Service.MESSAGING_SERVICE,
                                   Constants.MessagingSystem.CONTAINER_MEMORY_MB,
                                   Constants.MessagingSystem.CONTAINER_VIRTUAL_CORES,
                                   Constants.MessagingSystem.CONTAINER_INSTANCES,
                                   Constants.MessagingSystem.MAX_INSTANCES));
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      builder.add(new ExploreServiceResourceKeys(cConf,
                                                 Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                                 Constants.Explore.CONTAINER_MEMORY_MB,
                                                 Constants.Explore.CONTAINER_VIRTUAL_CORES));
    }
    return builder.build();
  }
}
