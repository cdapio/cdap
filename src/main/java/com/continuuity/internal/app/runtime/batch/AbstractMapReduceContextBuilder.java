package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.common.metrics.OverlordMetricsReporter;
import com.continuuity.data.DataFabric;
import com.continuuity.data.DataFabricImpl;
import com.continuuity.data.dataset.DataSetInstantiator;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.SynchronousTransactionAgent;
import com.continuuity.data.operation.executor.TransactionProxy;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.runtime.DataSets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
   * @param classLoader classloader to use
   * @param programLocation program location
   * @param inputDataSetName name of the input dataset if specified for this mapreduce job, null otherwise
   * @param inputSplits input splits if specified for this mapreduce job, null otherwise
   * @param outputDataSetName name of the output dataset if specified for this mapreduce job, null otherwise
   * @return instance of {@link BasicMapReduceContext}
   */
  public BasicMapReduceContext build(CConfiguration conf, String runId,
                                     ClassLoader classLoader,
                                     String programLocation,
                                     @Nullable String inputDataSetName,
                                     @Nullable List<Split> inputSplits,
                                     @Nullable String outputDataSetName) {
    Injector injector = createInjector();

    // Initializing Program
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Program program;
    try {
      program = new Program(locationFactory.create(programLocation));
    } catch (IOException e) {
      LOG.error("Could not init Program based on location: " + programLocation);
      throw Throwables.propagate(e);
    }

    // Initializing dataset context and hooking it up with mapreduce job transaction
    OperationExecutor opex = injector.getInstance(OperationExecutor.class);
    OperationContext opexContext = new OperationContext(program.getAccountId(), program.getApplicationId());
    // TODO: Pick up tx started when mapreduce job was submitted (requires missing implementation of long-running txs)
    TransactionProxy transactionProxy = new TransactionProxy();
    transactionProxy.setTransactionAgent(new SynchronousTransactionAgent(opex, opexContext));
    DataFabric dataFabric = new DataFabricImpl(opex, opexContext);
    DataSetInstantiator dataSetContext =
      new DataSetInstantiator(dataFabric, transactionProxy, classLoader);
    dataSetContext.setDataSets(Lists.newArrayList(program.getSpecification().getDataSets().values()));

    // Creating mapreduce job context
    MapReduceSpecification spec = program.getSpecification().getMapReduces().get(program.getProgramName());
    BasicMapReduceContext context =
      new BasicMapReduceContext(program, RunId.from(runId),
                                // NOTE: we are initializing all datasets of application, so that user is not required
                                //       to define all datasets used in Mapper and Reducer classes on MapReduceJob
                                //       class level
                                DataSets.createDataSets(
                                  dataSetContext, program.getSpecification().getDataSets().keySet()),
                                spec);

    // Setting extra context's configuration: mapreduce input and output
    if (inputDataSetName != null && inputSplits != null) {
      context.setInput((BatchReadable) dataSetContext.getDataSet(inputDataSetName), inputSplits);
    }
    if (outputDataSetName != null) {
      context.setOutput((BatchWritable) dataSetContext.getDataSet(outputDataSetName));
    }

    // Hooking up with logging and metrics systems
    // this is a hack for old logging system
    CAppender.logWriter = injector.getInstance(LogWriter.class);
    // this is a hack for old metrics system
    OverlordMetricsReporter.enable(1, TimeUnit.SECONDS, conf);

    return context;
  }

  /**
   * @return instance of {@link Injector} with bindings for current runtime environment
   */
  protected abstract Injector createInjector();
}
