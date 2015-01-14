/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.deploy;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.app.program.Archive;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import com.google.common.annotations.VisibleForTesting;
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
   * Constructor that accepts archive file as input to invoke configure.
   *
   * @param archive name of the archive file for which configure is invoked in-memory.
   */
  public InMemoryConfigurator(Id.Namespace id, Location archive) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(archive);
    this.archive = archive;
  }

  /**
   * Executes the <code>Application.configure</code> within the same JVM.
   * <p>
   * This method could be dangerous and should be used only in standalone mode.
   * </p>
   *
   * @return A instance of {@link ListenableFuture}.
   */
  @Override
  public ListenableFuture<ConfigResponse> config() {
    SettableFuture<ConfigResponse> result = SettableFuture.create();

    try {
      // Load the JAR using the JAR class load and load the manifest file.
      Manifest manifest = BundleJarUtil.getManifest(archive);
      Preconditions.checkArgument(manifest != null, "Failed to load manifest from %s", archive.toURI());
      String mainClassName = manifest.getMainAttributes().getValue(ManifestFields.MAIN_CLASS);
      Preconditions.checkArgument(mainClassName != null && !mainClassName.isEmpty(),
                                  "Main class attribute cannot be empty");

      File unpackedJarDir = Files.createTempDir();
      try {
        Object appMain = new Archive(BundleJarUtil.unpackProgramJar(archive, unpackedJarDir),
                                                                  mainClassName).getMainClass().newInstance();
        if (!(appMain instanceof Application)) {
          throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
                                                        appMain.getClass().getName()));
        }

        Application app = (Application) appMain;
        result.set(createResponse(app));
      } finally {
        removeDir(unpackedJarDir);
      }

      return result;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Futures.immediateFailedFuture(t);
    }
  }

  private ConfigResponse createResponse(Application app) {
    return new DefaultConfigResponse(0, CharStreams.newReaderSupplier(getSpecJson(app)));
  }

  @VisibleForTesting
  static final String getSpecJson(Application app) {
    // Now, we call configure, which returns application specification.
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
    app.configure(configurer, new ApplicationContext());
    ApplicationSpecification specification = configurer.createSpecification();

    // Convert the specification to JSON.
    // We write the Application specification to output file in JSON format.
    // TODO: The SchemaGenerator should be injected
    return ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification);
  }

  private void removeDir(File dir) {
    try {
      DirUtils.deleteDirectoryContents(dir);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", dir, e);
    }
  }
}
