/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.runtime.AbstractResourceReporter;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.net.InetAddress;

/**
 *
 */
public class WorkflowProgramRunner implements ProgramRunner {

  private final MapReduceProgramRunner mapReduceProgramRunner;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public WorkflowProgramRunner(MapReduceProgramRunner mapReduceProgramRunner,
                               ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService) {
    this.mapReduceProgramRunner = mapReduceProgramRunner;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    RunId runId = RunIds.generate();
    WorkflowDriver driver = new WorkflowDriver(program, runId, options, hostname, workflowSpec, mapReduceProgramRunner);

    ProgramResourceReporter resourceReporter = new WorkflowResourceReporter(program);
    // Controller needs to be created before starting the driver so that the state change of the driver
    // service can be fully captured by the controller.
    ProgramController controller =
      new WorkflowProgramController(program, driver, serviceAnnouncer, runId, resourceReporter);
    driver.start();

    return controller;
  }

  /**
   * Writes what the workflow uses as resources.
   */
  private class WorkflowResourceReporter extends AbstractResourceReporter {
    private static final int DEFAULT_MEMORY_USAGE = 512;
    private static final int DEFAULT_VCORE_USAGE = 1;
    private final Program program;

    WorkflowResourceReporter(Program program) {
      super(program, metricsCollectionService);
      this.program = program;
    }

    @Override
    public void reportResources() {
      // one for the 'application master' and one for the 'container' it spins up.
      sendMetrics(metricContextBase + "." + program.getName(), 1, DEFAULT_MEMORY_USAGE, DEFAULT_VCORE_USAGE);
      sendAppMasterMetrics(DEFAULT_MEMORY_USAGE, DEFAULT_VCORE_USAGE);
    }
  }
}
