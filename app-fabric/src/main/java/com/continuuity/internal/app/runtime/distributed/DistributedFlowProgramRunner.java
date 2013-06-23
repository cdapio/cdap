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
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.weave.api.WeavePreparer;
import com.continuuity.weave.api.WeaveRunner;
import com.continuuity.weave.api.logging.PrinterLogHandler;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Map;

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
    LOG.info("Launching distributed flow: " + program.getProgramName() + ":" + flowSpec.getName());

    // TODO (ENG-2313): deal with logging
    WeavePreparer preparer = weaveRunner.prepare(new FlowWeaveApplication(program, flowSpec, hConfFile, cConfFile))
               .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)));

    String escapedRuntimeArgs = "'" + new Gson().toJson(options.getUserArguments()) + "'";
    for (Map.Entry<String, FlowletDefinition> entry : flowSpec.getFlowlets().entrySet()) {
      preparer.withArguments(entry.getKey(),
                             String.format("--%s", RunnableOptions.JAR), program.getProgramJarLocation().getName());
      preparer.withArguments(entry.getKey(),
                             String.format("--%s", RunnableOptions.RUNTIME_ARGS), escapedRuntimeArgs);
    }

    return new FlowWeaveProgramController(program.getProgramName(), preparer.start()).startListen();
  }
}
