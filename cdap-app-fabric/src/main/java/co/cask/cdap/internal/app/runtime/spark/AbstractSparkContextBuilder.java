/*
 * Copyright © 2014 Cask Data, Inc.
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

import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.batch.BasicMapReduceContext;
import co.cask.cdap.internal.app.runtime.spark.inmemory.InMemorySparkContextBuilder;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Builds the {@link BasicSparkContext}.
 * Subclasses must override {@link #prepare()} method by providing Guice injector configured for running and starting
 * services specific to the environment. To release those resources subclass must override {@link #finish()}
 */
public abstract class AbstractSparkContextBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractSparkContextBuilder.class);

  /**
   * Build the instance of {@link BasicSparkContext}.
   *
   * @param runId            program run id
   * @param logicalStartTime The logical start time of the job.
   * @param workflowBatch    Tells whether the batch job is started by workflow.
   * @param runtimeArguments the runtime arguments
   * @param tx               transaction to use
   * @param classLoader      classloader to use
   * @param programLocation  program location
   * @return instance of {@link BasicMapReduceContext}
   */
  public BasicSparkContext build(String runId, long logicalStartTime, String workflowBatch, Arguments runtimeArguments,
                                 Transaction tx, ClassLoader classLoader, URI programLocation) {
    Injector injector = prepare();

    // Initializing Program
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Program program;
    try {
      program = Programs.create(locationFactory.create(programLocation), classLoader);
      //TODO: This should be changed when we support Spark in Workflow
    } catch (IOException e) {
      LOG.error("Could not init Program based on location: {}", programLocation, e);
      throw Throwables.propagate(e);
    }

    // Initializing dataset context and hooking it up with Spark job transaction

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    CConfiguration configuration = injector.getInstance(CConfiguration.class);

    ApplicationSpecification appSpec = program.getSpecification();

    MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    StreamAdmin streamAdmin = injector.getInstance(StreamAdmin.class);

    // Creating Spark job context
    SparkSpecification sparkSpec = program.getSpecification().getSpark().get(program.getName());
    BasicSparkContext context =
      new BasicSparkContext(program, RunIds.fromString(runId), runtimeArguments, appSpec.getDatasets().keySet(),
                            sparkSpec, logicalStartTime, workflowBatch, metricsCollectionService,
                            datasetFramework, configuration, discoveryServiceClient, streamAdmin);

    // propagating tx to all txAware guys
    // The tx is committed or aborted depending upon the job success by the ProgramRunner and DatasetRecordWriter
    for (TransactionAware txAware : context.getDatasetInstantiator().getTransactionAware()) {
      txAware.startTx(tx);
    }
    return context;
  }

  /**
   * Subclasses must override {@link #prepare()} method by providing Guice injector configured for running and starting
   * services specific to the environment. Like {@link InMemorySparkContextBuilder} does.
   *
   * @return instance of {@link Injector} with bindings for current runtime environment
   */
  protected abstract Injector prepare();

  /**
   * Subclass must override {@link #finish()} to release resources
   */
  protected void finish() {
    // Do NOTHING by default
  }
}
