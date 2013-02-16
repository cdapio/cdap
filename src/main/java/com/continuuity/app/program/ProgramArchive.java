/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.archive.JarResources;
import com.continuuity.filesystem.Location;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 *
 */
public final class ProgramArchive {

  private static final String PROCESSOR_TYPE = "Processor-Type";
  private static final String PROCESSOR_NAME = "Processor-Name";
  private static final String SPEC_FILE = "Spec-File";

  private final ClassLoader jarClassLoader;
  private final String mainClassName;
  private final Type processorType;
  private final String processorName;
  private final ApplicationSpecification specification;

  public ProgramArchive(File file) throws IOException {
    this(new JarResources(file));
  }

  public ProgramArchive(Location location) throws IOException {
    this(new JarResources(location));
  }

  private ProgramArchive(JarResources jarResources) throws IOException {
    jarClassLoader = new JarClassLoader(jarResources);

    Manifest manifest = jarResources.getManifest();

    mainClassName = manifest.getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
    check(mainClassName != null, "Fail to get %s attribute in jar.", Attributes.Name.MAIN_CLASS);

    String type = manifest.getMainAttributes().getValue(PROCESSOR_TYPE);
    processorType = type == null ? null : Type.valueOf(type);

    processorName = manifest.getMainAttributes().getValue(PROCESSOR_NAME);

    String appSpecFile = manifest.getMainAttributes().getValue(SPEC_FILE);
    specification = appSpecFile == null ? null : ApplicationSpecificationAdapter.create().fromJson(
      CharStreams.newReaderSupplier(
        ByteStreams.newInputStreamSupplier(jarResources.getResource(appSpecFile)), Charsets.UTF_8));
  }

  public Class<?> getMainClass() throws ClassNotFoundException {
    return jarClassLoader.loadClass(mainClassName);
  }

  public Type getProcessorType() {
    return processorType;
  }

  public String getProcessorName() {
    return processorName;
  }

  public ApplicationSpecification getSpecification() {
    return specification;
  }

  private void check(boolean condition, String fmt, Object...objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }
}
