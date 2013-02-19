package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.common.conf.Configuration;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.io.OutputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Locale;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 *
 */
public class ProgramGenerationStage extends AbstractStage<ApplicationSpecLocation> {
  private final Predicate<JarEntry> metaIgnore = new Predicate<JarEntry>() {
    @Override
    public boolean apply(@Nullable JarEntry input) {
      return input.getName().contains("MANIFEST.MF");
    }
  };
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
    String applicationName = o.getSpecification().getName();

    ArchiveBundler bundler = new ArchiveBundler(o.getArchive());

    // Create a tempoaray application specification
    Location appSpecDir = locationFactory.create(configuration.get("app.temp.dir", "/tmp") + "/"
                                                   + UUID.randomUUID() + "/" + System.nanoTime());
    if(!appSpecDir.mkdirs()) {
      throw new IOException("Failed to create directory");
    }
    Location appSpecFile = appSpecDir.append("application.json");

    final OutputStream os = appSpecFile.getOutputStream();
    try {
      ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
      ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(o.getSpecification()));
      adapter.toJson(o.getSpecification(), new OutputSupplier<Writer>() {
        @Override
        public Writer getOutput() throws IOException {
          return new OutputStreamWriter(os);
        }
      });
    } finally {
      if(os != null) {
        os.close();
      }
    }

    try {
      // Make sure we have a directory to store the original artifact.
      Location outputDir = locationFactory.create(configuration.get("app.output.dir"));
      Location newOutputDir = outputDir
        .append(o.getApplicationId().getAccountId())
        .append(applicationName);
      if(! newOutputDir.exists() && !newOutputDir.mkdirs()) {
        throw new IOException("Failed to create directory");
      }

      // Now, we iterate through FlowSpecification and generate programs
      for(FlowSpecification flow : o.getSpecification().getFlows().values()) {
        String name = String.format(Locale.ENGLISH, "%s.%s.%s", Type.FLOW.toString(), applicationName, flow.getName());
        Location output = newOutputDir.append(name + ".jar");
        Location loc = clone(o.getApplicationId(), bundler, output, flow.getName(),
                             flow.getClassName(), Type.FLOW, appSpecFile);
        PROGRAMS.add(new Program(loc));
      }

      // Iterate through ProcedureSpecification and generate program
      for(ProcedureSpecification procedure : o.getSpecification().getProcedures().values()) {
        String name = String.format(Locale.ENGLISH, "%s.%s.%s", Type.PROCEDURE.toString(),
                                    applicationName, procedure.getName());
        Location output = newOutputDir.append(name + ".jar");
        Location loc = clone(o.getApplicationId(), bundler, output, procedure.getName(),
                             procedure.getClassName(), Type.PROCEDURE, appSpecFile);
        PROGRAMS.add(new Program(loc));
      }

      // Iterate through MapRecduceSpecification and generate program
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

  /**
   * Clones a give application archive using the {@link ArchiveBundler}.
   * A new manifest file will be amended to the jar.
   *
   * @return An instance of {@link Location} containing the program JAR.
   *
   * @throws IOException in case of any issue related to copying jars.
   */
  private Location clone(Id.Application id, ArchiveBundler bundler, Location output, String programName,
                         String className, Type type, Location specFile)
    throws IOException {
    // Create a MANIFEST file
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, ManifestFields.VERSION);
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, className);
    manifest.getMainAttributes().put(ManifestFields.PROCESSOR_TYPE, type.toString());
    manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, ManifestFields.MANIFEST_SPEC_FILE);
    manifest.getMainAttributes().put(ManifestFields.ACCOUNT_ID, id.getAccountId());
    manifest.getMainAttributes().put(ManifestFields.APPLICATION_ID, id.getId());
    manifest.getMainAttributes().put(ManifestFields.PROGRAM_NAME, programName);
    bundler.clone(output, manifest, ImmutableList.of(specFile), metaIgnore);
    return output;
  }
}
