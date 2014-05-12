/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.continuuity.app.program.Archive;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.common.lang.jar.BundleJarUtil;
import com.continuuity.common.utils.DirUtils;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.jar.Manifest;

/**
 * In Memory Configurator doesn't spawn a external process, but
 * does this in memory.
 *
 * @see SandboxConfigurator
 */
public final class InMemoryConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryConfigurator.class);
  /**
   * JAR file path.
   */
  private final Location archive;

  /**
   * Application which needs to be configured.
   */
  private final Application application;

  /**
   * Constructor that accepts archive file as input to invoke configure.
   *
   * @param archive name of the archive file for which configure is invoked in-memory.
   */
  public InMemoryConfigurator(Id.Account id, Location archive) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(archive);
    this.archive = archive;
    this.application = null;
  }

  /**
   * Constructor that takes an {@link Application} to invoke configure.
   *
   * @param application instance for which configure needs to be invoked.
   */
  public InMemoryConfigurator(Id.Account id, Application application) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(application);
    this.archive = null;
    this.application = application;
  }

  /**
   * Executes the <code>Application.configure</code> within the same JVM.
   * <p>
   * This method could be dangerous and should be used only in singlenode.
   * </p>
   *
   * @return A instance of {@link ListenableFuture}.
   */
  @Override
  public ListenableFuture<ConfigResponse> config() {
    SettableFuture<ConfigResponse> result = SettableFuture.create();

    try {
      if (archive != null) {   // Provided Application JAR.
        // Load the JAR using the JAR class load and load the manifest file.
        Manifest manifest = BundleJarUtil.getManifest(archive);
        Preconditions.checkArgument(manifest != null, "Failed to load manifest from %s", archive.toURI());
        String mainClassName = manifest.getMainAttributes().getValue(ManifestFields.MAIN_CLASS);

        File unpackedJarDir = Files.createTempDir();
        try {
          Application app = new Archive(BundleJarUtil.unpackProgramJar(archive, unpackedJarDir),
                                        mainClassName).getMainClass().newInstance();
          result.set(createResponse(app));
        } finally {
          removeDir(unpackedJarDir);
        }
      } else {
        result.set(createResponse(application));
      }

      return result;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Futures.immediateFailedFuture(t);
    }
  }

  private ConfigResponse createResponse(Application app) {
    // Now, we call configure, which returns application specification.
    ApplicationSpecification specification = Specifications.from(app.configure());

    // Convert the specification to JSON.
    // TODO: The SchemaGenerator should be injected
    String specJson = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification);
    return new DefaultConfigResponse(0, CharStreams.newReaderSupplier(specJson));
  }

  private void removeDir(File dir) {
    try {
      DirUtils.deleteDirectoryContents(dir);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", dir, e);
    }
  }
}
