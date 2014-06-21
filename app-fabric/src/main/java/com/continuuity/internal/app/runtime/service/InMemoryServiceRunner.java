package com.continuuity.internal.app.runtime.service;

import com.continuuity.api.service.ServiceSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.internal.app.runtime.BasicArguments;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.ProgramRunnerFactory;
import com.continuuity.internal.app.runtime.SimpleProgramOptions;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.internal.RunIds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * For Running Services in Singlenode.
 */
public class InMemoryServiceRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryServiceRunner.class);
  private final Map<RunId, ProgramOptions> programOptions = Maps.newHashMap();
  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  InMemoryServiceRunner(ProgramRunnerFactory programRunnerFactory) {
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.SERVICE, "Only SERVICE process type is supported.");

    ServiceSpecification serviceSpec = appSpec.getServices().get(program.getName());
    Preconditions.checkNotNull(serviceSpec, "Missing ServiceSpecification for %s", program.getName());

    //RuIid for the service
    RunId runId = RunIds.generate();
    programOptions.put(runId, options);
    final Table<String, Integer, ProgramController> serviceRunnables = createRunnables(program, runId, serviceSpec);
    return new ServiceProgramController(serviceRunnables, runId, program, serviceSpec);
  }

  private Table<String, Integer, ProgramController> createRunnables(Program program, RunId runId,
                                                                    ServiceSpecification serviceSpec) {
    Table<String, Integer, ProgramController> runnables = HashBasedTable.create();

    try {
      for (Map.Entry<String, RuntimeSpecification> entry : serviceSpec.getRunnables().entrySet()) {
        int instanceCount = entry.getValue().getResourceSpecification().getInstances();
        for (int instanceId = 0; instanceId < instanceCount; instanceId++) {
          runnables.put(entry.getKey(), instanceId, startRunnable
            (program, createRunnableOptions(entry.getKey(), instanceId, instanceCount, runId)));
        }
      }
    } catch (Throwable t) {
      // Need to stop all started runnable here.
      try {
        Futures.successfulAsList
          (Iterables.transform(runnables.values(), new Function<ProgramController,
                                 ListenableFuture<ProgramController>>() {
                                 @Override
                                 public ListenableFuture<ProgramController> apply(ProgramController input) {
                                   return input.stop();
                                 }
                               }
           )
          ).get();
      } catch (Exception e) {
        LOG.error("Failed to stop all the runnables");
      }
      throw Throwables.propagate(t);
    }
    return runnables;
  }

  private ProgramController startRunnable(Program program, ProgramOptions runnableOptions) {
    return programRunnerFactory.create(ProgramRunnerFactory.Type.RUNNABLE).run(program, runnableOptions);
  }

  private ProgramOptions createRunnableOptions(String name, int instanceId, int instances, RunId runId) {

    // Get the right user arguments.
    Arguments userArguments = new BasicArguments();
    if (programOptions.containsKey(runId)) {
      userArguments = programOptions.get(runId).getUserArguments();
    }

    return new SimpleProgramOptions(name, new BasicArguments
      (ImmutableMap.of(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
                       ProgramOptionConstants.INSTANCES, Integer.toString(instances),
                       ProgramOptionConstants.RUN_ID, runId.getId())), userArguments);
  }

  class ServiceProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> runnables;
    private final ServiceSpecification serviceSpec;

    ServiceProgramController(Table<String, Integer, ProgramController> runnables,
                             RunId runId, Program program, ServiceSpecification serviceSpec) {
      super(program.getName(), runId);
      this.runnables = runnables;
      this.serviceSpec = serviceSpec;
      started();
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
      LOG.info("Stopping Service : " + serviceSpec.getName());
      Futures.successfulAsList
        (Iterables.transform(runnables.values(), new Function<ProgramController,
                               ListenableFuture<ProgramController>>() {
                               @Override
                               public ListenableFuture<ProgramController> apply(ProgramController input) {
                                 return input.stop();
                               }
                             }
         )
        ).get();
      LOG.info("Service stopped: " + serviceSpec.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {

    }
  }
}
