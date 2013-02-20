package com.continuuity.internal.app.runtime;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.Controller;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class FlowProgramRunner implements ProgramRunner {

  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  public FlowProgramRunner(ProgramRunnerFactory programRunnerFactory) {
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public Controller run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

    FlowSpecification flowSpec = appSpec.getFlows().get(program.getProgramName());
    Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getProgramName());

    // Launch flowlet program runners
    final List<Controller> controllers = Lists.newArrayListWithCapacity(flowSpec.getFlowlets().size());
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      for (int instanceId = 0; instanceId < entry.getValue().getInstances(); instanceId++) {
        controllers.add(programRunnerFactory.create().run(program, new FlowletOptions(entry.getKey(), instanceId)));
      }
    }

    return new Controller() {

      @Override
      public void suspend() {
        for (Controller controller : controllers) {
          controller.suspend();
        }
      }

      @Override
      public void resume() {
        for (Controller controller : controllers) {
          controller.resume();
        }
      }

      @Override
      public void stop() {
        for (Controller controller : controllers) {
          controller.stop();
        }
      }
    };
  }

  private final static class FlowletOptions implements ProgramOptions {

    private final String name;
    private final Arguments arguments;
    private final Arguments userArguments;

    private FlowletOptions(String name, int instanceId) {
      this.name = name;
      this.arguments = new BasicArguments(ImmutableMap.of("instanceId", Integer.toString(instanceId)));
      this.userArguments = new BasicArguments();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Arguments getArguments() {
      return arguments;
    }

    @Override
    public Arguments getUserArguments() {
      return userArguments;
    }
  }
}
