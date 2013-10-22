package com.continuuity.app.program;

import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.archive.JarClassLoader;
import com.continuuity.archive.JarResources;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Default implementation of program.
 */
final class DefaultProgram implements Program {
  private final ClassLoader jarClassLoader;
  private final String mainClassName;
  private final Type processorType;
  private final ApplicationSpecification specification;
  private final Id.Program id;
  // TODO: should not be exposed at this level of abstraction. Added to support first cut of MapReduce integration
  private final Location programJarLocation;

  DefaultProgram(Location location, JarResources jarResources) throws IOException {
    this.programJarLocation = location;

    jarClassLoader = new JarClassLoader(jarResources);

    Manifest manifest = jarResources.getManifest();

    mainClassName = getAttribute(manifest, ManifestFields.MAIN_CLASS);

    String accountId = getAttribute(manifest, ManifestFields.ACCOUNT_ID);
    String applicationId = getAttribute(manifest, ManifestFields.APPLICATION_ID);
    String programName = getAttribute(manifest, ManifestFields.PROGRAM_NAME);
    id = Id.Program.from(accountId, applicationId, programName);

    String type = getAttribute(manifest, ManifestFields.PROCESSOR_TYPE);
    processorType = type == null ? null : Type.valueOf(type);

    String appSpecFile = getAttribute(manifest, ManifestFields.SPEC_FILE);

    specification = appSpecFile == null ? null : ApplicationSpecificationAdapter.create()
      .fromJson(CharStreams.newReaderSupplier(
        ByteStreams.newInputStreamSupplier(jarResources.getResource(appSpecFile)),
        Charsets.UTF_8)
      );
  }

  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) jarClassLoader.loadClass(mainClassName);
  }

  @Override
  public Type getType() {
    return processorType;
  }

  @Override
  public Id.Program getId() {
    return id;
  }

  @Override
  public String getName() {
    return id.getId();
  }

  @Override
  public String getAccountId() {
    return id.getAccountId();
  }

  @Override
  public String getApplicationId() {
    return id.getApplicationId();
  }

  @Override
  public ApplicationSpecification getSpecification() {
    return specification;
  }

  @Override
  public Location getJarLocation() {
    return programJarLocation;
  }

  @Override
  public ClassLoader getClassLoader() {
    return jarClassLoader;
  }

  private String getAttribute(Manifest manifest, Attributes.Name name) throws IOException {
    Preconditions.checkNotNull(manifest);
    Preconditions.checkNotNull(name);
    String value = manifest.getMainAttributes().getValue(name);
    check(value != null, "Fail to get %s attribute from jar", name);
    return value;
  }

  private void check(boolean condition, String fmt, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }
}
