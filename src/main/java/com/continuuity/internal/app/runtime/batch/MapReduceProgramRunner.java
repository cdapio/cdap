package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.continuuity.base.Cancellable;
import com.continuuity.common.logging.common.LogWriter;
import com.continuuity.common.logging.logback.CAppender;
import com.continuuity.data.dataset.DataSetContext;
import com.continuuity.data.operation.executor.TransactionAgent;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.internal.app.runtime.AbstractListener;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataFabricFacade;
import com.continuuity.internal.app.runtime.DataFabricFacadeFactory;
import com.continuuity.internal.app.runtime.DataSets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs {@link com.continuuity.api.batch.MapReduce} programs
 */
public class MapReduceProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceProgramRunner.class);

  private final MapReduceRuntimeService mapReduceRuntimeService;
  private final DataFabricFacadeFactory txAgentSupplierFactory;
  private final OVCTableHandle tableHandle;

  @Inject
  public MapReduceProgramRunner(MapReduceRuntimeService mapReduceRuntimeService,
                                DataFabricFacadeFactory txAgentSupplierFactory,
                                LogWriter logWriter, OVCTableHandle tableHandle) {

    this.mapReduceRuntimeService = mapReduceRuntimeService;
    this.txAgentSupplierFactory = txAgentSupplierFactory;
    this.tableHandle = tableHandle;
    CAppender.logWriter = logWriter;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduces().get(program.getProgramName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getProgramName());

    DataFabricFacade txAgentSupplier = txAgentSupplierFactory.createDataFabricFacadeFactory(program);
    DataSetContext dataSetContext = txAgentSupplier.getDataSetContext();

    // TODO: integrate with long-running transactions
    final TransactionAgent txAgent = txAgentSupplier.createAndUpdateTransactionAgentProxy();
    try {
      txAgent.start();
    } catch(OperationException e) {
      throw Throwables.propagate(e);
    }

    try {
      RunId runId = RunId.generate();
      final BasicMapReduceContext context =
        new BasicMapReduceContext(program, runId, DataSets.createDataSets(dataSetContext, tableHandle, spec.getDataSets()), spec);

      MapReduce job = (MapReduce) program.getMainClass().newInstance();
      context.injectFields(job);

      final MapReduceProgramController controller = new MapReduceProgramController(context);

      LOG.info("Starting MapReduce job: " + context.toString() +
                 " using MapReduceRuntimeService: " + mapReduceRuntimeService);
      final Cancellable jobCancellable =
        mapReduceRuntimeService.submit(job, program.getProgramJarLocation(), context,
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

  private static final class MapReduceProgramController extends AbstractProgramController {
    MapReduceProgramController(BasicMapReduceContext context) {
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
      // When job is stopped by controller doStop() method, the stopping() method of listener is also called.
      // That is where we kill the job, so no need to do any extra job in doStop().
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      // No-op
    }
  }
}
