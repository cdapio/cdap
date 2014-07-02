package com.continuuity.internal.app.program;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import org.apache.twill.filesystem.Location;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.Manifest;

/**
 *
 */
public final class ProgramBundle {
  private static final String APPLICATION_META_ENTRY = "application.json";
  private static final Predicate<JarEntry> META_IGNORE = new Predicate<JarEntry>() {
    @Override
    public boolean apply(JarEntry input) {
      return input.getName().contains("MANIFEST.MF") || input.getName().contains(APPLICATION_META_ENTRY);
    }
  };

  /**
   * Clones a give application archive using the {@link com.continuuity.archive.ArchiveBundler}.
   * A new manifest file will be amended to the jar.
   *
   * @return An instance of {@link Location} containing the program JAR.
   *
   * @throws java.io.IOException in case of any issue related to copying jars.
   */
  public static Location create(Id.Application id, ArchiveBundler bundler, Location output, String programName,
                                String className, Type type, ApplicationSpecification appSpec) throws IOException {
    return create(id, bundler, output, programName, className, type, appSpec, null);
  }

  public static Location create(Id.Application id, ArchiveBundler bundler, Location output, String programName,
                               String className, Type type, ApplicationSpecification appSpec,
                               Manifest other) throws IOException {
    // Create a MANIFEST file
    Manifest manifest = new Manifest();

    // Copy over attributes from other manifest
    if (other != null) {
      for (Map.Entry<Object, Object> entry : other.getMainAttributes().entrySet()) {
        Attributes.Name key = (Attributes.Name) entry.getKey();
        String value = (String) entry.getValue();
        manifest.getMainAttributes().put(key, value);
      }
    }

    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, ManifestFields.VERSION);
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, className);
    manifest.getMainAttributes().put(ManifestFields.PROCESSOR_TYPE, type.toString());
    manifest.getMainAttributes().put(ManifestFields.SPEC_FILE, ManifestFields.MANIFEST_SPEC_FILE);
    manifest.getMainAttributes().put(ManifestFields.ACCOUNT_ID, id.getAccountId());
    manifest.getMainAttributes().put(ManifestFields.APPLICATION_ID, id.getId());
    manifest.getMainAttributes().put(ManifestFields.PROGRAM_NAME, programName);

    bundler.clone(output, manifest, ImmutableMap.of(APPLICATION_META_ENTRY, getInputSupplier(appSpec)), META_IGNORE);
    return output;
  }

  private static InputSupplier<InputStream> getInputSupplier(final ApplicationSpecification appSpec) {
    return new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        String json = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(appSpec);
        return new ByteArrayInputStream(json.getBytes(Charsets.UTF_8));
      }
    };
  }
}
