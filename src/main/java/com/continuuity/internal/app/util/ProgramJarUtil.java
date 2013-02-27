package com.continuuity.internal.app.util;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.io.OutputSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;

/**
 *
 */
public final class ProgramJarUtil {
  private static final Predicate<JarEntry> META_IGNORE = new Predicate<JarEntry>() {
    @Override
    public boolean apply(@Nullable JarEntry input) {
      return input.getName().contains("MANIFEST.MF") || input.getName().contains("application.json");
    }
  };

  /**
   * Clones a give application archive using the {@link com.continuuity.archive.ArchiveBundler}.
   * A new manifest file will be amended to the jar.
   *
   * @return An instance of {@link com.continuuity.filesystem.Location} containing the program JAR.
   *
   * @throws java.io.IOException in case of any issue related to copying jars.
   */
  public static Location clone(Id.Application id, ArchiveBundler bundler, Location output, String programName,
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
    bundler.clone(output, manifest, ImmutableList.of(specFile), META_IGNORE);
    return output;
  }

  public static void write(ApplicationSpecification appSpec, Location destination) throws IOException {
    final OutputStream os = destination.getOutputStream();
    try {
      ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
      adapter.toJson(appSpec, new OutputSupplier<Writer>() {
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
  }

}
