/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
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
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

/**
 * Runs Mapreduce programm in distributed environment
 */
public final class DistributedMapReduceProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedMapReduceProgramRunner.class);

  @Inject
  public DistributedMapReduceProgramRunner(WeaveRunner weaveRunner, Configuration hConf, CConfiguration cConf) {
    super(weaveRunner, hConf, cConf);
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    Type processorType = program.getProcessorType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == Type.MAPREDUCE, "Only MAPREDUCE process type is supported.");

    MapReduceSpecification spec = appSpec.getMapReduces().get(program.getProgramName());
    Preconditions.checkNotNull(spec, "Missing MapReduceSpecification for %s", program.getProgramName());

    LOG.info("Launching distributed flow: " + program.getProgramName() + ":" + spec.getName());

    String escapedRuntimeArgs = "'" + new Gson().toJson(options.getUserArguments()) + "'";
    // TODO (ENG-2526): deal with logging
    WeavePreparer preparer
      = weaveRunner.prepare(new MapReduceWeaveApplication(program, spec, hConfFile, cConfFile))
          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
          .withArguments(spec.getName(),
                         String.format("--%s", RunnableOptions.JAR), program.getProgramJarLocation().getName())
          .withArguments(spec.getName(),
                         String.format("--%s", RunnableOptions.RUNTIME_ARGS), escapedRuntimeArgs);

    return new MapReduceWeaveProgramController(program.getProgramName(), preparer.start()).startListen();
  }
}
