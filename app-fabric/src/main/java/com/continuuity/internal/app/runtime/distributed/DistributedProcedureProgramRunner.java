/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.MetricsCollectionService;
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
public final class DistributedProcedureProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedProcedureProgramRunner.class);
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public DistributedProcedureProgramRunner(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf,
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
    Preconditions.checkArgument(processorType == Type.PROCEDURE, "Only PROCEDURE process type is supported.");

    ProcedureSpecification procedureSpec = appSpec.getProcedures().get(program.getName());
    Preconditions.checkNotNull(procedureSpec, "Missing ProcedureSpecification for %s", program.getName());

    LOG.info("Launching distributed flow: " + program.getName() + ":" + procedureSpec.getName());

    String runtimeArgs = new Gson().toJson(options.getUserArguments());
    // TODO (ENG-2526): deal with logging
    WeavePreparer preparer
      = weaveRunner.prepare(new ProcedureWeaveApplication(program, procedureSpec, hConfFile, cConfFile))
          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
          .withArguments(procedureSpec.getName(),
                         String.format("--%s", RunnableOptions.JAR), program.getJarLocation().getName())
          .withArguments(procedureSpec.getName(),
                         String.format("--%s", RunnableOptions.RUNTIME_ARGS), runtimeArgs);

    return new ProcedureWeaveProgramController(program.getName(), preparer.start()).startListen();
  }
}
