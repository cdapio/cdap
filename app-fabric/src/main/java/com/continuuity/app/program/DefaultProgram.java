package com.continuuity.app.program;

import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.common.lang.ApiResourceListHolder;
import com.continuuity.common.lang.ClassLoaders;
import com.continuuity.common.lang.jar.BundleJarUtil;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Default implementation of program.
 */
public final class DefaultProgram implements Program {

  private final String mainClassName;
  private final Type processorType;

  private final Id.Program id;

  private final Location programJarLocation;
  private final File expandFolder;
  private final ClassLoader parentClassLoader;
  private final File specFile;
  private boolean expanded;
  private ClassLoader classLoader;
  private ApplicationSpecification specification;

  /**
   * Creates a program instance.
   *
   * @param programJarLocation Location of the program jar file.
   * @param expandFolder Local directory for expanding the jar file into. If it is {@code null},
   *                     the {@link #getClassLoader()} methods would throw exception.
   * @param parentClassLoader Parent classloader for the program class.
   */
  DefaultProgram(Location programJarLocation,
                 @Nullable File expandFolder, ClassLoader parentClassLoader) throws IOException {
    this.programJarLocation = programJarLocation;
    this.expandFolder = expandFolder;
    this.parentClassLoader = parentClassLoader;

    Manifest manifest = BundleJarUtil.getManifest(programJarLocation);
    if (manifest == null) {
      throw new IOException("Failed to load manifest in program jar from " + programJarLocation.toURI());
    }

    mainClassName = getAttribute(manifest, ManifestFields.MAIN_CLASS);
    id = Id.Program.from(getAttribute(manifest, ManifestFields.ACCOUNT_ID),
                         getAttribute(manifest, ManifestFields.APPLICATION_ID),
                         getAttribute(manifest, ManifestFields.PROGRAM_NAME));

    processorType = Type.valueOf(getAttribute(manifest, ManifestFields.PROCESSOR_TYPE));

    // Load the app spec from the jar file if no expand folder is provided. Otherwise do lazy loading after the jar
    // is expanded.
    String specPath = getAttribute(manifest, ManifestFields.SPEC_FILE);
    if (expandFolder == null) {
      specification = ApplicationSpecificationAdapter.create().fromJson(
        CharStreams.newReaderSupplier(BundleJarUtil.getEntry(programJarLocation, specPath), Charsets.UTF_8));
      specFile = null;
    } else {
      specFile = new File(expandFolder, specPath);
    }
  }

  public DefaultProgram(Location programJarLocation, ClassLoader classLoader) throws IOException {
    this(programJarLocation, null, null);
    this.classLoader = classLoader;
  }

  @Override
  public String getMainClassName() {
    return mainClassName;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Class<T> getMainClass() throws ClassNotFoundException {
    return (Class<T>) getClassLoader().loadClass(mainClassName);
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
  public synchronized ApplicationSpecification getSpecification() {
    if (specification == null) {
      expandIfNeeded();
      try {
        specification = ApplicationSpecificationAdapter.create().fromJson(
          CharStreams.newReaderSupplier(Files.newInputStreamSupplier(specFile), Charsets.UTF_8));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return specification;
  }

  @Override
  public Location getJarLocation() {
    return programJarLocation;
  }

  @Override
  public synchronized ClassLoader getClassLoader() {
    if (classLoader == null) {
      expandIfNeeded();
      try {
        classLoader = ClassLoaders.newProgramClassLoader(
          expandFolder, ApiResourceListHolder.getResourceList(), parentClassLoader);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
    return classLoader;
  }

  private String getAttribute(Manifest manifest, Attributes.Name name) throws IOException {
    String value = manifest.getMainAttributes().getValue(name);
    check(value != null, "Fail to get %s attribute from jar", name);
    return value;
  }

  private void check(boolean condition, String fmt, Object... objs) throws IOException {
    if (!condition) {
      throw new IOException(String.format(fmt, objs));
    }
  }

  private synchronized void expandIfNeeded() {
    if (expanded) {
      return;
    }

    Preconditions.checkState(expandFolder != null, "Directory for jar expansion is not defined.");

    try {
      BundleJarUtil.unpackProgramJar(programJarLocation, expandFolder);
      expanded = true;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
