package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.app.metrics.MapReduceMetrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabric2Impl;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data2.transaction.Transaction;
import com.continuuity.data2.transaction.TransactionAware;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.workflow.WorkflowMapReduceProgram;
import com.continuuity.logging.appender.LogAppenderInitializer;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Builds the {@link BasicMapReduceContext}.
 * Subclasses should override {@link #createInjector()} method by providing Guice injector configured for running
 * environment.
 */
public abstract class AbstractMapReduceContextBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMapReduceContextBuilder.class);

  /**
   * Build the instance of {@link BasicMapReduceContext}.
   * @param conf runtime configuration
   * @param runId program run id
   * @param logicalStartTime The logical start time of the job.
   * @param workflowBatch Tells whether the batch job is started by workflow.
   * @param tx transaction to use
   * @param classLoader classloader to use
   * @param programLocation program location
   * @param inputDataSetName name of the input dataset if specified for this mapreduce job, null otherwise
   * @param inputSplits input splits if specified for this mapreduce job, null otherwise
   * @param outputDataSetName name of the output dataset if specified for this mapreduce job, null otherwise
   * @return instance of {@link BasicMapReduceContext}
   */
  public BasicMapReduceContext build(CConfiguration conf,
                                     MapReduceMetrics.TaskType type,
                                     String runId,
                                     long logicalStartTime,
                                     String workflowBatch,
                                     Arguments runtimeArguments,
                                     Transaction tx,
                                     ClassLoader classLoader,
                                     URI programLocation,
                                     @Nullable String inputDataSetName,
                                     @Nullable List<Split> inputSplits,
                                     @Nullable String outputDataSetName) {
    Injector injector = createInjector();

    // Initializing Program
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Program program;
    try {
      program = loadProgram(programLocation, locationFactory);
      // See if it is launched from Workflow, if it is, change the Program.
      if (workflowBatch != null) {
        MapReduceSpecification mapReduceSpec = program.getSpecification().getMapReduce().get(workflowBatch);
        Preconditions.checkArgument(mapReduceSpec != null, "Cannot find MapReduceSpecification for %s", workflowBatch);
        program = new WorkflowMapReduceProgram(program, mapReduceSpec);
      }
    } catch (IOException e) {
      LOG.error("Could not init Program based on location: " + programLocation);
      throw Throwables.propagate(e);
    }

    // Initializing dataset context and hooking it up with mapreduce job transaction

    DataSetAccessor dataSetAccessor = injector.getInstance(DataSetAccessor.class);

    DataFabric dataFabric = new DataFabric2Impl(locationFactory, dataSetAccessor);
    DataSetInstantiator dataSetContext =
      new DataSetInstantiator(dataFabric, classLoader);
    dataSetContext.setDataSets(program.getSpecification().getDataSets().values());

    // if this is not for a mapper or a reducer, we don't need the metrics collection service
    MetricsCollectionService metricsCollectionService =
      (type == null) ? null : injector.getInstance(MetricsCollectionService.class);

    // creating dataset instances earlier so that we can pass them to txAgent
    // NOTE: we are initializing all datasets of application, so that user is not required
    //       to define all datasets used in Mapper and Reducer classes on MapReduceJob
    //       class level
    Map<String, DataSet> dataSets = DataSets.createDataSets(
      dataSetContext, program.getSpecification().getDataSets().keySet());

    // Creating mapreduce job context
    MapReduceSpecification spec = program.getSpecification().getMapReduce().get(program.getName());
    BasicMapReduceContext context =
      new BasicMapReduceContext(program, type, RunIds.fromString(runId),
                                runtimeArguments, dataSets, spec,
                                dataSetContext.getTransactionAware(), logicalStartTime,
                                workflowBatch, metricsCollectionService);

    if (type == MapReduceMetrics.TaskType.Mapper) {
      dataSetContext.setMetricsCollector(context.getSystemMapperMetrics());
    } else if (type == MapReduceMetrics.TaskType.Reducer) {
      dataSetContext.setMetricsCollector(context.getSystemReducerMetrics());
    }

    // propagating tx to all txAware guys
    // NOTE: tx will be committed by client code
    for (TransactionAware txAware : dataSetContext.getTransactionAware()) {
      txAware.startTx(tx);
    }

    // Setting extra context's configuration: mapreduce input and output
    if (inputDataSetName != null && inputSplits != null) {
      context.setInput((BatchReadable) context.getDataSet(inputDataSetName), inputSplits);
    }
    if (outputDataSetName != null) {
      context.setOutput((BatchWritable) context.getDataSet(outputDataSetName));
    }

    // Initialize log appender
    LogAppenderInitializer logAppenderInitializer = injector.getInstance(LogAppenderInitializer.class);
    logAppenderInitializer.initialize();

    return context;
  }

  protected Program loadProgram(URI programLocation, LocationFactory locationFactory) throws IOException {
    return Programs.create(locationFactory.create(programLocation));
  }

  /**
   * @return instance of {@link Injector} with bindings for current runtime environment
   */
  protected abstract Injector createInjector();
}
