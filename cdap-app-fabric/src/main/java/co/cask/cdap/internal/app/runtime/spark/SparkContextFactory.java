/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionSystemClient;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.Map;

/**
 * Factory for creating {@link ExecutionSparkContext} by referencing properties in {@link ClientSparkContext}.
 */
final class SparkContextFactory {

  private final Configuration hConf;
  private final ClientSparkContext clientContext;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txClient;
  private final StreamAdmin streamAdmin;

  SparkContextFactory(Configuration hConf, ClientSparkContext clientContext,
                      DatasetFramework datasetFramework, TransactionSystemClient txClient, StreamAdmin streamAdmin) {
    this.hConf = hConf;
    this.clientContext = clientContext;
    this.datasetFramework = datasetFramework;
    this.txClient = txClient;
    this.streamAdmin = streamAdmin;
  }

  /**
   * Returns the {@link ClientSparkContext}.
   */
  ClientSparkContext getClientContext() {
    return clientContext;
  }

  /**
   * Creates a new instance of {@link ExecutionSparkContext} based on the {@link ClientSparkContext} in this factory.
   *
   * @param transaction the transaction for the spark execution
   * @param localizedResources the resources localized for the spark program
   */
  ExecutionSparkContext createExecutionContext(Transaction transaction, Map<String, File> localizedResources) {
    SparkSpecification spec = updateSpecExecutorResources(clientContext.getSpecification(),
                                                          clientContext.getExecutorResources());
    return new ExecutionSparkContext(clientContext.getApplicationSpecification(), spec, clientContext.getProgramId(),
                                     clientContext.getRunId(), clientContext.getProgramClassLoader(),
                                     clientContext.getRuntimeArguments(),
                                     transaction, datasetFramework, txClient, clientContext.getDiscoveryServiceClient(),
                                     clientContext.getMetricsContext(), clientContext.getLoggingContext(),
                                     hConf, streamAdmin, localizedResources,
                                     clientContext.getPluginInstantiator(), clientContext.getWorkflowProgramInfo());
  }

  private SparkSpecification updateSpecExecutorResources(SparkSpecification originalSpec, Resources executorResources) {
    return new SparkSpecification(originalSpec.getClassName(), originalSpec.getName(), originalSpec.getDescription(),
                                  originalSpec.getMainClassName(), originalSpec.getProperties(),
                                  originalSpec.getDriverResources(), executorResources);
  }
}
