/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.runtime;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.app.runtime.RunId;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class FlowProgramRunner implements ProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlowProgramRunner.class);

  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  public FlowProgramRunner(ProgramRunnerFactory programRunnerFactory) {
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.FLOW, "Only FLOW process type is supported.");

    FlowSpecification flowSpec = appSpec.getFlows().get(program.getProgramName());
    Preconditions.checkNotNull(flowSpec, "Missing FlowSpecification for %s", program.getProgramName());

    // Launch flowlet program runners
    final Table<String, Integer, ProgramController> flowlets = createFlowlets(program, flowSpec);
    return new FlowProgramController(flowlets, program, flowSpec);
  }

  /**
   * Starts all flowlets in the flow program.
   * @param program Program to run
   * @param flowSpec The {@link FlowSpecification}.
   * @return A {@link Table} with row as flowlet id, column as instance id, cell as the {@link ProgramController}
   *         for the flowlet.
   */
  private Table<String, Integer, ProgramController> createFlowlets(Program program, FlowSpecification flowSpec) {
    Table<String, Integer, ProgramController> flowlets = HashBasedTable.create();

    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      for (int instanceId = 0; instanceId < entry.getValue().getInstances(); instanceId++) {
        flowlets.put(entry.getKey(), instanceId, startFlowlet(program, entry.getKey(), instanceId));
      }
    }
    return flowlets;
  }

  private ProgramController startFlowlet(Program program, String flowletName, int instanceId) {
    return programRunnerFactory.create(ProgramRunnerFactory.Type.FLOWLET)
                               .run(program, new FlowletOptions(flowletName, instanceId));
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

  private final class FlowProgramController extends AbstractProgramController {

    private final Table<String, Integer, ProgramController> flowlets;
    private final Program program;
    private final FlowSpecification flowSpec;
    private final Lock lock = new ReentrantLock();

    FlowProgramController(Table<String, Integer, ProgramController> flowlets,
                          Program program, FlowSpecification flowSpec) {
      super(program.getProgramName(), RunId.generate());
      this.flowlets = flowlets;
      this.program = program;
      this.flowSpec = flowSpec;
    }

    @Override
    protected void doSuspend() throws Exception {
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.suspend();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }

    }

    @Override
    protected void doResume() throws Exception {
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.resume();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }
    }

    @Override
    protected void doStop() throws Exception {
      lock.lock();
      try {
        Futures.successfulAsList(
          Iterables.transform(flowlets.values(),
                              new Function<ProgramController, ListenableFuture<ProgramController>>() {
                                @Override
                                public ListenableFuture<ProgramController> apply(ProgramController input) {
                                  return input.stop();
                                }
                              })).get();
      } finally {
        lock.unlock();
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doCommand(String name, Object value) throws Exception {
      if (!"instance".equals(name) || !(value instanceof Map)) {
        return;
      }
      Map<String, Integer> command = (Map<String, Integer>)value;
      lock.lock();
      try {
        for (Map.Entry<String, Integer> entry : command.entrySet()) {
          String flowletName = entry.getKey();
          Map<Integer, ProgramController> liveFlowlets = flowlets.row(flowletName);
          int instances = entry.getValue();
          int liveCount = liveFlowlets.size();

          if (liveCount == instances) {
            // Same number of instances, no change
            continue;
          }
          if (liveCount < instances) {
            // Increate number of instances
            for (int instanceId = liveCount; instanceId < instances; instanceId++) {
              flowlets.put(flowletName, instanceId, startFlowlet(program, flowletName, instanceId));
            }
          } else {
            // Decrease number of instances
            List<ListenableFuture<?>> futures = Lists.newArrayListWithCapacity(liveCount - instances);
            for (int instanceId = liveCount - 1; instanceId >= instanceId; instanceId--) {
              ProgramController controller = flowlets.remove(flowletName, instanceId);
              if (controller != null) {
                futures.add(controller.stop());
              }
            }
            Futures.successfulAsList(futures).get();
          }
        }
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
      } finally {
        lock.unlock();
      }
    }
  }
}
