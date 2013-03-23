package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.hadoop.HadoopMapReduceJob;
import com.continuuity.api.batch.hadoop.HadoopMapReduceJobSpecification;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.continuuity.base.Cancellable;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.TransactionAgentSupplier;
import com.continuuity.internal.app.runtime.TransactionAgentSupplierFactory;
import com.continuuity.internal.app.runtime.batch.BasicBatchContext;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * Runs {@link HadoopMapReduceJob} programs
 */
public class MapReduceProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final MapReduceRuntimeService mapReduceRuntimeService;
  private final TransactionAgentSupplierFactory txAgentSupplierFactory;

  @Inject
  public MapReduceProgramRunner(MapReduceRuntimeService mapReduceRuntimeService,
                                TransactionAgentSupplierFactory txAgentSupplierFactory) {
    this.mapReduceRuntimeService = mapReduceRuntimeService;
    this.txAgentSupplierFactory = txAgentSupplierFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    HadoopMapReduceJobSpecification spec = appSpec.getMapReduceJobs().get(program.getProgramName());
    Preconditions.checkNotNull(spec, "Missing HadoopMapReduceJobSpecification for %s", program.getProgramName());

    TransactionAgentSupplier txAgentSupplier = txAgentSupplierFactory.create(program);
    DataSetContext dataSetContext = txAgentSupplier.getDataSetContext();

    // TODO: integrate with long-running transactions
    final TransactionAgent txAgent = txAgentSupplier.createAndUpdateProxy();
    try {
      txAgent.start();
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }

    try {
      RunId runId = RunId.generate();
      final BasicBatchContext context = new BasicBatchContext(program, runId,
                                                        DataSets.createDataSets(dataSetContext, spec.getDataSets()));

      HadoopMapReduceJob job = (HadoopMapReduceJob) program.getMainClass().newInstance();
      injectFields(job, TypeToken.of(job.getClass()), context);

      final MapReduceProgramController controller = new MapReduceProgramController(context);

      LOG.info("Starting MapReduce job: " + context.toString() +
                 " using MapReduceRuntimeService: " + mapReduceRuntimeService);
      final Cancellable jobCancellable =
        mapReduceRuntimeService.submit(job, spec, program.getProgramJarLocation(), context,
          // using callback to stop controller when mapreduce job is finished
          // (also to finish transaction, but that might change after integration with "long running transactions")
                                       new MapReduceRuntimeService.JobFinishCallback() {
          @Override
          public void onFinished(boolean success) {
            controller.stop();
            try {
              if (success) {
                txAgent.finish();
              } else {
                txAgent.abort();
              }
            } catch (OperationException e) {
              throw Throwables.propagate(e);
            }
          }
        });

      // adding listener which stops mapreduce job when controller stops.
      controller.addListener(new AbstractListener() {
        @Override
        public void stopping() {
          LOG.info("Stopping mapreduce job: " + context);
          jobCancellable.cancel();
          LOG.info("Mapreduce job stopped: " + context);
        }
      }, MoreExecutors.sameThreadExecutor());

      return controller;

    } catch (Throwable e) {
      try {
        txAgent.abort();
      } catch (OperationException ex) {
        throw Throwables.propagate(ex);
      }
      throw Throwables.propagate(e);
    }
  }

  // TODO: duplicate code in ProcedureProgramRunner
  private void injectFields(Object injectTo, TypeToken<?> typeToken,
                            BasicBatchContext context) {

    // Walk up the hierarchy of the class.
    for (TypeToken<?> type : typeToken.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Metrics fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset != null && !dataset.value().isEmpty()) {
            setField(injectTo, field, context.getDataSet(dataset.value()));
          }
          continue;
        }
        if (Metrics.class.equals(field.getType())) {
          setField(injectTo, field, context.getMetrics());
        }
      }
    }
  }

  private void setField(Object setTo, Field field, Object value) {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    try {
      field.set(setTo, value);
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  private static final class MapReduceProgramController extends AbstractProgramController {
    MapReduceProgramController(BasicBatchContext context) {
      super(context.getProgramName(), context.getRunId());
    }

    @Override
    protected void doSuspend() throws Exception {
      // No-op
    }

    @Override
    protected void doResume() throws Exception {
      // No-op
    }

    @Override
    protected void doStop() throws Exception {
      // do nothing here: we stop mapreduce job in listener
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      // No-op
    }
  }
}
