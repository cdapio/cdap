/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.meta.CheckpointManagerFactory;
import com.google.inject.Inject;

/**
 * Factory to create {@link LogMetricsPlugin}.
 */
public class LogMetricsPluginFactory implements KafkaLogProcessorFactory {
  private final MetricsCollectionService metricsCollectionService;
  private final CheckpointManagerFactory checkpointManagerFactory;
  private final CConfiguration cConfig;

  @Inject
  public LogMetricsPluginFactory(MetricsCollectionService metricsCollectionService,
                                 CheckpointManagerFactory checkpointManagerFactory, CConfiguration cConfig) {
    this.metricsCollectionService = metricsCollectionService;
    this.checkpointManagerFactory = checkpointManagerFactory;
    this.cConfig = cConfig;
  }

  @Override
  public KafkaLogProcessor create() throws Exception {
    return new LogMetricsPlugin(metricsCollectionService, checkpointManagerFactory, cConfig);
  }
}
