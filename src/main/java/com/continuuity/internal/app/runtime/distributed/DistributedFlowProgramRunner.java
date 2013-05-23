/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.RunId;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public final class DistributedFlowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedFlowProgramRunner.class);

  @Inject
  DistributedFlowProgramRunner(WeaveRunner weaveRunner, Configuration hConfig, CConfiguration cConfig) {
    super(weaveRunner, hConfig, cConfig);
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
    RunId runId = RunId.generate();

    LOG.info("Launching distributed flow: " + program.getProgramName() + ":" + flowSpec.getName());

    WeavePreparer preparer = weaveRunner.prepare(new FlowWeaveApplication(program, flowSpec, hConfFile, cConfFile))
               .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));   // TODO(terence): deal with logging

    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      preparer.withArguments(entry.getKey(),
                             "--jar", program.getProgramJarLocation().getName(),
                             "--instances", Integer.toString(entry.getValue().getInstances()),
                             "--runId", runId.getId());
    }

    final WeaveController controller = preparer.start();

    return new FlowProgramController(program, runId, controller);
  }

  private static class FlowProgramController extends AbstractProgramController {

    private final Lock lock;
    private final WeaveController controller;

    public FlowProgramController(Program program, RunId runId, WeaveController controller) {
      super(program.getProgramName(), runId);
      this.controller = controller;
      lock = new ReentrantLock();
    }

    @Override
    protected void doSuspend() throws Exception {
      controller.sendCommand(ProgramCommands.SUSPEND).get();
    }

    @Override
    protected void doResume() throws Exception {
      controller.sendCommand(ProgramCommands.RESUME).get();
    }

    @Override
    protected void doStop() throws Exception {
      controller.stopAndWait();
    }

    @Override
    protected void doCommand(String name, Object value) throws Exception {
      if (!"instances".equals(name) || !(value instanceof Map)) {
        return;
      }
      Map<String, Integer> command = (Map<String, Integer>)value;
      lock.lock();
      try {
        for (Map.Entry<String, Integer> entry : command.entrySet()) {
          changeInstances(entry.getKey(), entry.getValue());
        }
      } catch (Throwable t) {
        LOG.error(String.format("Fail to change instances: %s", command), t);
      } finally {
        lock.unlock();
      }
    }

    /**
     * Change the number of instances of the running flowlet. Notice that this method needs to be
     * synchronized as change of instances involves multiple steps that need to be completed all at once.
     * @param flowletName Name of the flowlet
     * @param newInstanceCount New instance count
     * @throws java.util.concurrent.ExecutionException
     * @throws InterruptedException
     */
    private synchronized void changeInstances(String flowletName, final int newInstanceCount) throws Exception {
      // TODO
    }
  }
}
