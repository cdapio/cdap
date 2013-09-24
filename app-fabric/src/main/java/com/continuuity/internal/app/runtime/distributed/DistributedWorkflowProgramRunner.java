/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramResourceReporter;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.weave.api.WeaveController;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

/**
 *
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public DistributedWorkflowProgramRunner(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf,
                                          MetricsCollectionService metricsCollectionService) {
    super(weaveRunner, hConf, cConf);
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    LOG.info("Launching distributed workflow: " + program.getName() + ":" + workflowSpec.getName());

    String runtimeArgs = new Gson().toJson(options.getUserArguments());
    // TODO (ENG-2526): deal with logging
    WeavePreparer preparer
      = weaveRunner.prepare(new WorkflowWeaveApplication(program, workflowSpec, hConfFile, cConfFile))
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .withArguments(workflowSpec.getName(),
                     String.format("--%s", RunnableOptions.JAR), program.getJarLocation().getName())
      .withArguments(workflowSpec.getName(),
                     String.format("--%s", RunnableOptions.RUNTIME_ARGS), runtimeArgs);
    WeaveController controller = preparer.start();
    ProgramResourceReporter resourceReporter =
      new DistributedResourceReporter(program, metricsCollectionService, controller);

    return new WorkflowWeaveProgramController(program.getName(), controller, resourceReporter).startListen();
  }
}
