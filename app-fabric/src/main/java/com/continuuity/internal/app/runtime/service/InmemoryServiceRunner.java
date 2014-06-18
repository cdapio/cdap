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
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.internal.RunIds;

import java.util.Map;

/**
 * Runs Service in single-node
 */
public class InmemoryServiceRunner implements ProgramRunner {

  private final Map<RunId, ProgramOptions> programOptions = Maps.newHashMap();
  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  InmemoryServiceRunner(ProgramRunnerFactory programRunnerFactory) {
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

    RunId runId = RunIds.generate();
    final Table<String, Integer, ProgramController> serviceRunnables = createRunnables(program, runId, serviceSpec);
    return new ServiceProgramController(serviceRunnables, runId, program, serviceSpec);
  }

  private Table<String, Integer, ProgramController> createRunnables(Program program, RunId runId,
                                                                    ServiceSpecification serviceSpec) {
    Table<String, Integer, ProgramController> runnables = HashBasedTable.create();

    try {
      for (Map.Entry<String, RuntimeSpecification> entry : serviceSpec.getRunnables().entrySet()) {
        // instancecount will be in context, may be we can pass that from context to twillRunnableSpecification
        int instanceCount = Integer.parseInt(entry.getValue().getRunnableSpecification().getConfigs().get("instance"));
        for (int instanceId = 0; instanceId < instanceCount; instanceId++) {
          runnables.put(entry.getKey(), instanceId, startRunnable(program,
                                                                  createRunnableOptions(entry.getKey(), instanceId,
                                                                                        instanceCount, runId)));
        }
      }
    } catch (Throwable t) {
        // Need to stop all started runnable here.
      throw Throwables.propagate(t);
    }
    return runnables;
  }

  private ProgramController startRunnable(Program program, ProgramOptions runnableOptions) {
    return programRunnerFactory.create(ProgramRunnerFactory.Type.RUNNABLE).run(program, runnableOptions);
  }


  private ProgramOptions createRunnableOptions(String name, int instanceId, int instances,
                                               RunId runId) {

    // Get the right user arguments.
    Arguments userArguments = new BasicArguments();
    if (programOptions.containsKey(runId)) {
      userArguments = programOptions.get(runId).getUserArguments();
    }

    return new SimpleProgramOptions(name, new BasicArguments(
      ImmutableMap.of(ProgramOptionConstants.INSTANCE_ID, Integer.toString(instanceId),
                      ProgramOptionConstants.INSTANCES, Integer.toString(instances),
                      ProgramOptionConstants.RUN_ID, runId.getId()
      ))
      , userArguments);
  }

  class ServiceProgramController extends AbstractProgramController {
    private final Table<String, Integer, ProgramController> runnables;
    private final Program program;
    private final ServiceSpecification serviceSpec;


    public ServiceProgramController(Table<String, Integer, ProgramController> runnables, RunId runId,
                             Program program, ServiceSpecification serviceSpec) {
      super(program.getName(), runId);
      this.runnables = runnables;
      this.program = program;
      this.serviceSpec = serviceSpec;
      started();
    }

    @Override
    protected void doSuspend() throws Exception {
      //can iterate through the ProgramControllers for runnables and stop them
    }

    @Override
    protected void doResume() throws Exception {

    }

    @Override
    protected void doStop() throws Exception {

    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {

    }

    private synchronized void changeInstances(String runnableName, final int newInstanceCount) throws Exception {

    }

  }
}
