package com.continuuity.internal.app.runtime.procedure;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetContext;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.DataSetContextFactory;
import com.continuuity.internal.app.runtime.DataSets;
import com.continuuity.internal.app.runtime.TransactionAgentSupplier;
import com.continuuity.internal.app.runtime.TransactionAgentSupplierFactory;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.lang.reflect.Field;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 */
public final class ProcedureProgramRunner implements ProgramRunner {

  private static final int MAX_WORKER_THREADS = 10;

  private ServerBootstrap bootstrap;
  private final TransactionAgentSupplierFactory txAgentSupplierFactory;
  private final DataSetContextFactory dataSetContextFactory;

  @Inject
  public ProcedureProgramRunner(TransactionAgentSupplierFactory txAgentSupplierFactory,
                                DataSetContextFactory dataSetContextFactory) {
    this.txAgentSupplierFactory = txAgentSupplierFactory;
    this.dataSetContextFactory = dataSetContextFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {
      // Extract and verify parameters
      ApplicationSpecification appSpec = program.getSpecification();
      Preconditions.checkNotNull(appSpec, "Missing application specification.");

      Type processorType = program.getProcessorType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.PROCEDURE, "Only PROCEDURE process type is supported.");

      ProcedureSpecification procedureSpec = appSpec.getProcedures().get(program.getProgramName());
      Preconditions.checkNotNull(procedureSpec, "Missing ProcedureSpecification for %s", program.getProgramName());

      int instanceId = Integer.parseInt(options.getArguments().getOption("instanceId", "0"));

      Class<? extends Procedure> procedureClass = (Class<? extends Procedure>) program.getMainClass();
      ClassLoader classLoader = procedureClass.getClassLoader();

      RunId runId = RunId.generate();

      // Creates opex related objects
      TransactionAgentSupplier txAgentSupplier = txAgentSupplierFactory.create(program);
      DataSetContext dataSetContext = dataSetContextFactory.create(program);

      BasicProcedureContext procedureContext =
        new BasicProcedureContext(program, instanceId, runId,
                                  DataSets.createDataSets(dataSetContext, procedureSpec.getDataSets()),
                                  procedureSpec);

      bootstrap = createBootstrap(program);

      return null;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private ServerBootstrap createBootstrap(Program program) {
    // Single thread for boss thread
    Executor bossExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("procedure-boss-" + program.getProgramName() + "-%d")
        .build());

    // Worker threads pool
    Executor workerExecutor = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("procedure-worker-" + program.getAccountId() + "-%d")
        .build());

    ServerBootstrap bootstrap = new ServerBootstrap(
                                    new NioServerSocketChannelFactory(bossExecutor,
                                                                      workerExecutor,
                                                                      MAX_WORKER_THREADS));

    // Setup processing pipeline

    return bootstrap;
  }


  private void injectFields(Procedure procedure, TypeToken<? extends Procedure> procedureType,
                            BasicProcedureContext procedureContext) throws IllegalAccessException {

    // Walk up the hierarchy of procedure class.
    for (TypeToken<?> type : procedureType.getTypes().classes()) {
      if (type.getRawType().equals(Object.class)) {
        break;
      }

      // Inject DataSet and Metrics fields.
      for (Field field : type.getRawType().getDeclaredFields()) {
        // Inject DataSet
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset != null && !dataset.value().isEmpty()) {
            setField(procedure, field, procedureContext.getDataSet(dataset.value()));
          }
          continue;
        }
        if (Metrics.class.equals(field.getType())) {
          setField(procedure, field, procedureContext.getMetrics());
        }
      }
    }
  }

  private void setField(Procedure procedure, Field field, Object value) throws IllegalAccessException {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    field.set(procedure, value);
  }

  private final class ProcedureProgramController extends AbstractProgramController {

    ProcedureProgramController(Program program, RunId runId) {
      super(program.getProgramName(), runId);
    }

    @Override
    protected void doSuspend() throws Exception {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doResume() throws Exception {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doStop() throws Exception {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      //To change body of implemented methods use File | Settings | File Templates.
    }
  }
}
