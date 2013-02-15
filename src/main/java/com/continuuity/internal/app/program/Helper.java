/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.app.program.Type;
import com.continuuity.archive.ArchiveBundler;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.filesystem.Location;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

/**
 *
 */
public class Helper {
  /**
   * Defines an attribute Spec file.
   */
  private static final Attributes.Name SPEC_FILE = new Attributes.Name("Spec-File");

  /**
   * Defines an attribute program type
   */
  private static final Attributes.Name PROCESSOR_TYPE = new Attributes.Name("Processor-Type");

  private static final String MANIFEST_SPEC_FILE = "META-INF/specification/application.json";

  public static List<Location> getProgramArchives(Location archive, Location outputDir,
                                                  ApplicationSpecification specification)
    throws IOException {
    List<Location> outputs = Lists.newLinkedList();
    String applicationName = specification.getName();

    ArchiveBundler bundler = new ArchiveBundler(archive);

    Predicate<ZipEntry> ignoreMeta = new Predicate<ZipEntry>() {
      @Override
      public boolean apply(@Nullable ZipEntry input) {
        return input.getName().contains("MANIFEST.MF");
      }
    };

    Writer writer = null;

    try {
      File appSpecFile = File.createTempFile("application", "spec");
      writer = Files.newWriter(appSpecFile, Charsets.UTF_8);
      ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification, writer);
      // Add the file to the list to be included in the jar.
      File[] additionFiles = new File[]{appSpecFile};

      // Create
      for(Map.Entry<String, FlowSpecification> entry : specification.getFlows().entrySet()) {
        String name = String.format(Locale.ENGLISH, "%s.%s", applicationName, entry.getKey());
        Location output = outputDir.append(name);
        String klass = entry.getValue().getName();
      }
    } finally {
      if(writer != null) {
        writer.close();
      }
    }
    return Collections.unmodifiableList(outputs);
  }

  private static Location cloneProgram(ArchiveBundler bundler, Location output, String klass, Type type, Location specFile)
    throws IOException {
    // Create a MANIFEST file
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, klass.getClass());
    manifest.getMainAttributes().put(PROCESSOR_TYPE, type.toString());
    manifest.getMainAttributes().put(SPEC_FILE, MANIFEST_SPEC_FILE);

    bundler.clone(output, manifest, new Location[] { specFile },new Predicate<ZipEntry>() {
      @Override
      public boolean apply(@Nullable ZipEntry input) {
        return input.getName().contains("MANIFEST.MF");
      }
    });
    return output;
  }
}
