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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.batch.MapReduceProgramRunner;
import co.cask.cdap.internal.app.runtime.spark.SparkProgramRunner;
import co.cask.cdap.proto.ProgramType;
import co.cask.tephra.TransactionSystemClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Factory for instantiating the programs that are going to run inside a {@link Workflow}.
 */
public class WorkflowProgramRunnerFactory implements ProgramRunnerFactory {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final LocationFactory locationFactory;
  private final StreamAdmin streamAdmin;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;
  private final MetricsCollectionService metricsCollectionService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Store store;
  private final UsageRegistry usageRegistry;

  public WorkflowProgramRunnerFactory(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                                      StreamAdmin streamAdmin, DatasetFramework datasetFramework,
                                      TransactionSystemClient txSystemClient,
                                      MetricsCollectionService metricsCollectionService,
                                      DiscoveryServiceClient discoveryServiceClient, Store store,
                                      UsageRegistry usageRegistry) {
    this.cConf = cConf;
    this.hConf = hConf;
    this.locationFactory = locationFactory;
    this.streamAdmin = streamAdmin;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.store = store;
    this.usageRegistry = usageRegistry;
  }

  @Override
  public ProgramRunner create(ProgramType programType) {
    switch(programType) {
      case MAPREDUCE:
        return new MapReduceProgramRunner(cConf, hConf, locationFactory, streamAdmin, datasetFramework, txSystemClient,
                                          metricsCollectionService, discoveryServiceClient, store, usageRegistry);
      case SPARK:
        return new SparkProgramRunner(cConf, hConf, txSystemClient, datasetFramework, metricsCollectionService,
                                      discoveryServiceClient, streamAdmin, store);
      default:
        throw new IllegalArgumentException(String.format("Program of type %s cannot be run by the Workflow.",
                                                         programType));

    }
  }
}
