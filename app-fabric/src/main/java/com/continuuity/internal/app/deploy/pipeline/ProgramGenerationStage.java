package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.ProgramSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.Configuration;
import com.continuuity.common.conf.Constants;
import com.continuuity.internal.app.program.ProgramBundle;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.pipeline.AbstractStage;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

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

    // Now, we iterate through all ProgramSpecification and generate programs
    Iterable<ProgramSpecification> specifications = Iterables.concat(
      appSpec.getMapReduce().values(),
      appSpec.getFlows().values(),
      appSpec.getProcedures().values(),
      appSpec.getWorkflows().values()
    );

    for (ProgramSpecification spec: specifications) {
      Type type = Type.typeOfSpecification(spec);
      String name = String.format(Locale.ENGLISH, "%s/%s", type, applicationName);
      Location programDir = newOutputDir.append(name);
      if (!programDir.exists()) {
        programDir.mkdirs();
      }
      Location output = programDir.append(String.format("%s.jar", spec.getName()));
      Location loc = ProgramBundle.create(o.getApplicationId(), bundler, output, spec.getName(), spec.getClassName(),
                                          type, appSpec);
      programs.add(Programs.create(loc));
    }

    // TODO: webapp information should come from webapp spec.
    // Generate webapp program if required
    Set<String> servingHostNames = WebappProgramRunner.getServingHostNames(o.getArchive().getInputStream());

    if (!servingHostNames.isEmpty()) {
      Type type = Type.WEBAPP;
      String name = String.format(Locale.ENGLISH, "%s/%s", type, applicationName);
      Location programDir = newOutputDir.append(name);

      if (!programDir.exists()) {
        programDir.mkdirs();
      }

      String programName = type.name().toLowerCase();
      Location output = programDir.append(String.format("%s.jar", programName));
      Location loc = ProgramBundle.create(o.getApplicationId(), bundler, output, programName, "", type, appSpec);
      programs.add(Programs.create(loc));
    }

    // Emits the received specification with programs.
    emit(new ApplicationWithPrograms(o, programs.build()));
  }
}
