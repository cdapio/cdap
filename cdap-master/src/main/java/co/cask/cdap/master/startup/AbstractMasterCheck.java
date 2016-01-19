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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.startup.Check;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Base for master startup checks.
 */
abstract class AbstractMasterCheck extends Check {
  protected final CConfiguration cConf;
  protected final Set<ServiceResourceKeys> systemServicesResourceKeys;

  protected AbstractMasterCheck(CConfiguration cConf) {
    this.cConf = cConf;
    ImmutableSet.Builder<ServiceResourceKeys> builder = ImmutableSet.<ServiceResourceKeys>builder()
      .add(new ServiceResourceKeys(Constants.Service.TRANSACTION,
                                   Constants.Transaction.Container.MEMORY_MB,
                                   Constants.Transaction.Container.NUM_CORES,
                                   Constants.Transaction.Container.NUM_INSTANCES,
                                   Constants.Transaction.Container.MAX_INSTANCES))
      .add(new ServiceResourceKeys(Constants.Service.STREAMS,
                                   Constants.Stream.CONTAINER_MEMORY_MB,
                                   Constants.Stream.CONTAINER_VIRTUAL_CORES,
                                   Constants.Stream.CONTAINER_INSTANCES,
                                   Constants.Stream.MAX_INSTANCES))
      .add(new ServiceResourceKeys(Constants.Service.METRICS,
                                   Constants.Metrics.MEMORY_MB,
                                   Constants.Metrics.NUM_CORES,
                                   Constants.Metrics.NUM_INSTANCES,
                                   Constants.Metrics.MAX_INSTANCES))
      .add(new ServiceResourceKeys(Constants.Service.METRICS_PROCESSOR,
                                   Constants.MetricsProcessor.MEMORY_MB,
                                   Constants.MetricsProcessor.NUM_CORES,
                                   Constants.MetricsProcessor.NUM_INSTANCES,
                                   Constants.MetricsProcessor.MAX_INSTANCES))
      .add(new ServiceResourceKeys(Constants.Service.LOGSAVER,
                                   Constants.LogSaver.MEMORY_MB,
                                   Constants.LogSaver.NUM_CORES,
                                   Constants.LogSaver.NUM_INSTANCES,
                                   Constants.LogSaver.MAX_INSTANCES));
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      builder.add(new ServiceResourceKeys(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                          Constants.Explore.CONTAINER_MEMORY_MB,
                                          Constants.Explore.CONTAINER_VIRTUAL_CORES,
                                          Constants.Explore.CONTAINER_INSTANCES,
                                          Constants.Explore.MAX_INSTANCES));
    }
    this.systemServicesResourceKeys = builder.build();
  }
}
