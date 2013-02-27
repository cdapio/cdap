package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.Configuration;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.util.ProgramJarUtil;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;

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
    ImmutableList.Builder<Program> PROGRAMS = ImmutableList.builder();
    ApplicationSpecification appSpec = o.getSpecification();
    String applicationName = appSpec.getName();

    ArchiveBundler bundler = new ArchiveBundler(o.getArchive());

    // Create a tempoaray application specification
    Location appSpecDir = locationFactory.create(configuration.get("app.temp.dir", "/tmp") + "/"
                                                   + UUID.randomUUID() + "/" + System.nanoTime());
    if(!appSpecDir.mkdirs()) {
      throw new IOException("Failed to create directory");
    }
    Location appSpecFile = appSpecDir.append("application.json");

    ProgramJarUtil.write(appSpec, appSpecFile);

    try {
      // Make sure we have a directory to store the original artifact.
      Location outputDir = locationFactory.create(configuration.get("app.output.dir", "/tmp"));
      Location newOutputDir = outputDir
        .append(o.getApplicationId().getAccountId());
      if(! newOutputDir.exists() && !newOutputDir.mkdirs()) {
        throw new IOException("Failed to create directory");
      }

      // Now, we iterate through FlowSpecification and generate programs
      for(FlowSpecification flow : appSpec.getFlows().values()) {
        String name = String.format(Locale.ENGLISH, "%s/%s", Type.FLOW.toString(), applicationName);
        Location flowAppDir = newOutputDir.append(name);
        if(! flowAppDir.exists()) {
          flowAppDir.mkdirs();
        }
        Location output = flowAppDir.append(String.format("%s.jar", flow.getName()));
        Location loc = ProgramJarUtil.clone(o.getApplicationId(), bundler, output, flow.getName(), flow.getClassName(),
                                            Type.FLOW, appSpecFile);
        PROGRAMS.add(new Program(loc));
      }

      // Iterate through ProcedureSpecification and generate program
      for(ProcedureSpecification procedure : appSpec.getProcedures().values()) {
        String name = String.format(Locale.ENGLISH, "%s/%s", Type.PROCEDURE.toString(), applicationName);
        Location procedureAppDir = newOutputDir.append(name);
        if(! procedureAppDir.exists()) {
          procedureAppDir.mkdirs();
        }
        Location output = procedureAppDir.append(String.format("%s.jar", procedure.getName()));
        Location loc = ProgramJarUtil.clone(o.getApplicationId(), bundler, output, procedure.getName(), procedure.getClassName(), Type.PROCEDURE, appSpecFile);
        PROGRAMS.add(new Program(loc));
      }

      // Iterate through MapReduceSpecification and generate program
      // ...

    } finally {
      if(appSpecDir != null && appSpecDir.exists()) {
        appSpecDir.delete();
      }
      if(appSpecFile != null && appSpecFile.exists()) {
        appSpecFile.delete();
      }
    }

    // Emits the received specification with programs.
    emit(new ApplicationWithPrograms(o, PROGRAMS.build()));
  }
}
