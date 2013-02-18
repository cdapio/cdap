package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
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
  private static final String MANIFEST_SPEC_FILE = "META-INF/specification/application.json";
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
    Location appSpecFile = locationFactory.create(File.createTempFile("application", ".spec.json").getPath());
    OutputStream os = appSpecFile.getOutputStream();
    try {
      ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
      ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(o.getSpecification()));
//      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(o.getSpecification(),
//                                                                                     new OutputStreamWriter(os));
    } finally {
      if(os != null) {
        os.close();
      }
    }

    // Now, we iterate through FlowSpecification and generate programs
    Location outputDir = locationFactory.create(configuration.get("app.output.dir"));
    for(FlowSpecification flow : o.getSpecification().getFlows().values()) {
      String name = String.format(Locale.ENGLISH, "%s.%s", applicationName, flow.getName());
      Location newOutputDir = outputDir
        .append(o.getApplicationId().getAccountId())
        .append(applicationName);
      if(! newOutputDir.exists() && !newOutputDir.mkdirs()) {
        throw new IOException("Failed to create directory");
      }
      Location output = newOutputDir.append(name + ".jar");
      clone(bundler, output, flow.getClass(), Type.FLOW, appSpecFile);
    }
  }

  private Location clone(ArchiveBundler bundler, Location output, Class<?> klass, Type type, Location specFile)
    throws IOException {
    // Create a MANIFEST file
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, klass.getClass().getCanonicalName());
    manifest.getMainAttributes().put(ManifestFields.PROCESSOR_TYPE, type.toString());
    manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, MANIFEST_SPEC_FILE);
    bundler.clone(output, manifest, ImmutableList.of(specFile), metaIgnore);
    return output;
  }
}
