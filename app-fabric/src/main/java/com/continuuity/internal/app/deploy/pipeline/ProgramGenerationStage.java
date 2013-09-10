package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.Configuration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.program.ProgramBundle;
import com.continuuity.pipeline.AbstractStage;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Locale;

/**
 *
 */
public class ProgramGenerationStage extends AbstractStage<ApplicationSpecLocation> {
  private final LocationFactory locationFactory;
  private final Configuration configuration;

  public ProgramGenerationStage(Configuration configuration, LocationFactory locationFactory) {
    super(TypeToken.of(ApplicationSpecLocation.class));
    this.configuration = configuration;
    this.locationFactory = locationFactory;
  }

  @Override
  public void process(final ApplicationSpecLocation o) throws Exception {
    ImmutableList.Builder<Program> programs = ImmutableList.builder();
    ApplicationSpecification appSpec = o.getSpecification();
    String applicationName = appSpec.getName();

    ArchiveBundler bundler = new ArchiveBundler(o.getArchive());

    // Make sure we have a directory to store the original artifact.
    Location outputDir = locationFactory.create(configuration.get(Constants.AppFabric.OUTPUT_DIR));
    Location newOutputDir = outputDir.append(o.getApplicationId().getAccountId());

    // Check exists, create, check exists again to avoid failure due to race condition.
    if (!newOutputDir.exists() && !newOutputDir.mkdirs() && !newOutputDir.exists()) {
      throw new IOException("Failed to create directory");
    }

    // Now, we iterate through FlowSpecification and generate programs
    for (FlowSpecification flow : appSpec.getFlows().values()) {
      String name = String.format(Locale.ENGLISH, "%s/%s", Type.FLOW.toString(), applicationName);
      Location flowAppDir = newOutputDir.append(name);
      if (!flowAppDir.exists()) {
        flowAppDir.mkdirs();
      }
      Location output = flowAppDir.append(String.format("%s.jar", flow.getName()));
      Location loc = ProgramBundle.create(o.getApplicationId(), bundler, output, flow.getName(), flow.getClassName(),
                                         Type.FLOW, appSpec);
      programs.add(new Program(loc));
    }

    // Iterate through ProcedureSpecification and generate program
    for (ProcedureSpecification procedure : appSpec.getProcedures().values()) {
      String name = String.format(Locale.ENGLISH, "%s/%s", Type.PROCEDURE.toString(), applicationName);
      Location procedureAppDir = newOutputDir.append(name);
      if (!procedureAppDir.exists()) {
        procedureAppDir.mkdirs();
      }
      Location output = procedureAppDir.append(String.format("%s.jar", procedure.getName()));
      Location loc = ProgramBundle.create(o.getApplicationId(), bundler, output, procedure.getName(),
                                         procedure.getClassName(), Type.PROCEDURE, appSpec);
      programs.add(new Program(loc));
    }

    // Iterate through MapReduceSpecification and generate program
    for (MapReduceSpecification job : appSpec.getMapReduces().values()) {
      String name = String.format(Locale.ENGLISH, "%s/%s", Type.MAPREDUCE.toString(), applicationName);
      Location jobAppDir = newOutputDir.append(name);
      if (!jobAppDir.exists()) {
        jobAppDir.mkdirs();
      }
      Location output = jobAppDir.append(String.format("%s.jar", job.getName()));
      Location loc = ProgramBundle.create(o.getApplicationId(), bundler, output, job.getName(),
                                         job.getClassName(), Type.MAPREDUCE, appSpec);
      programs.add(new Program(loc));
    }

    // ...

    // Emits the received specification with programs.
    emit(new ApplicationWithPrograms(o, programs.build()));
  }
}
