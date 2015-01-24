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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowMapReduceProgram;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.RunIds;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Builds the {@link BasicMapReduceContext}.
 * Subclasses must override {@link #prepare()} method by providing Guice injector configured for running and starting
 * services specific to the environment. To release those resources subclass must override {@link #finish()}
 * environment.
 */
public abstract class AbstractMapReduceContextBuilder {

  /**
   * Build the instance of {@link BasicMapReduceContext}.
   *
   * @param runId program run id
   * @param logicalStartTime The logical start time of the job.
   * @param workflowBatch Tells whether the batch job is started by workflow.
   * @param tx transaction to use
   * @param mrProgram program containing the MapReduce job
   * @param inputDataSetName name of the input dataset if specified for this mapreduce job, null otherwise
   * @param inputSplits input splits if specified for this mapreduce job, null otherwise
   * @param outputDataSetName name of the output dataset if specified for this mapreduce job, null otherwise
   * @return instance of {@link BasicMapReduceContext}
   */
  public BasicMapReduceContext build(MapReduceMetrics.TaskType type,
                                     String runId,
                                     String taskId,
                                     long logicalStartTime,
                                     String workflowBatch,
                                     Arguments runtimeArguments,
                                     Transaction tx,
                                     Program mrProgram,
                                     @Nullable String inputDataSetName,
                                     @Nullable List<Split> inputSplits,
                                     @Nullable String outputDataSetName) {
    Injector injector = prepare();

    // Initializing Program
    Program program = mrProgram;

    // See if it was launched from Workflow; if it was, change the Program.
    if (workflowBatch != null) {
      MapReduceSpecification mapReduceSpec = program.getApplicationSpecification().getMapReduce().get(workflowBatch);
      Preconditions.checkArgument(mapReduceSpec != null, "Cannot find MapReduceSpecification for %s", workflowBatch);
      program = new WorkflowMapReduceProgram(program, mapReduceSpec);
    }

    // Initializing dataset context and hooking it up with mapreduce job transaction

    DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    CConfiguration configuration = injector.getInstance(CConfiguration.class);

    ApplicationSpecification programSpec = program.getApplicationSpecification();

    // if this is not for a mapper or a reducer, we don't need the metrics collection service
    MetricsCollectionService metricsCollectionService =
      (type == null) ? null : injector.getInstance(MetricsCollectionService.class);

    DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);

    // Creating mapreduce job context
    MapReduceSpecification spec = program.getApplicationSpecification().getMapReduce().get(program.getName());
    BasicMapReduceContext context =
      new BasicMapReduceContext(program, type, RunIds.fromString(runId), taskId,
                                runtimeArguments, programSpec.getDatasets().keySet(), spec, logicalStartTime,
                                workflowBatch, discoveryServiceClient, metricsCollectionService,
                                datasetFramework, configuration);

    // propagating tx to all txAware guys
    // NOTE: tx will be committed by client code
    for (TransactionAware txAware : context.getDatasetInstantiator().getTransactionAware()) {
      txAware.startTx(tx);
    }

    // Setting extra context's configuration: mapreduce input and output
    if (inputDataSetName != null && inputSplits != null) {
      context.setInput(inputDataSetName, inputSplits);
    }
    if (outputDataSetName != null) {
      context.setOutput(outputDataSetName);
    }

    return context;
  }

  /**
   * Refer to {@link AbstractMapReduceContextBuilder} for usage details
   * @return instance of {@link Injector} with bindings for current runtime environment
   */
  protected abstract Injector prepare();

  /**
   * Refer to {@link AbstractMapReduceContextBuilder} for usage details
   */
  protected void finish() {
    // Do NOTHING by default
  }
}
