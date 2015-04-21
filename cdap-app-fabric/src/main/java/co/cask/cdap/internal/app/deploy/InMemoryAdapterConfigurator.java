/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.app.program.Archive;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.templates.AdapterSpecification;
import co.cask.cdap.templates.DefaultAdapterConfigurer;
import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.jar.Manifest;

/**
 * In Memory Configurator doesn't spawn a external process, but
 * does this in memory.
 *
 * @see SandboxConfigurator
 */
public final class InMemoryAdapterConfigurator implements Configurator {
  private static final Gson GSON = ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryAdapterConfigurator.class);
  /**
   * JAR file path.
   */
  private final Location archive;
  private final AdapterConfig adapterConfig;
  private final ApplicationSpecification templateSpec;
  private final String adapterName;
  private final Id.Namespace namespaceId;

  public InMemoryAdapterConfigurator(Id.Namespace id, Location archive, String adapterName,
                                     AdapterConfig adapterConfig, ApplicationSpecification templateSpec) {
    Preconditions.checkNotNull(id);
    Preconditions.checkNotNull(archive);
    this.namespaceId = id;
    this.archive = archive;
    this.adapterConfig = adapterConfig;
    this.templateSpec = templateSpec;
    this.adapterName = adapterName;
  }

  /**
   * Executes the <code>ApplicationTemplate.configureAdapter</code> within the same JVM.
   * <p>
   * This method could be dangerous and should be used only in standalone mode.
   * </p>
   *
   * @return A instance of {@link ListenableFuture}.
   */
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
        if (!(appMain instanceof ApplicationTemplate)) {
          throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
                                                        appMain.getClass().getName()));
        }

        ApplicationTemplate template = (ApplicationTemplate) appMain;
        DefaultAdapterConfigurer configurer = new DefaultAdapterConfigurer(namespaceId, adapterName, adapterConfig,
                                                                           templateSpec);

        TypeToken typeToken = TypeToken.of(template.getClass());
        TypeToken<?> resultToken = typeToken.resolveType(ApplicationTemplate.class.getTypeParameters()[0]);
        Type configType;
        // if the user parameterized their template, like 'xyz extends ApplicationTemplate<T>',
        // we can deserialize the config into that object. Otherwise it'll just be an Object
        if (resultToken.getType() instanceof Class) {
          configType = resultToken.getType();
        } else {
          configType = Object.class;
        }
        template.configureAdapter(adapterName, GSON.fromJson(adapterConfig.getConfig(), configType), configurer);
        AdapterSpecification spec = configurer.createSpecification();
        result.set(new DefaultConfigResponse(0, CharStreams.newReaderSupplier(GSON.toJson(spec))));
      } finally {
        removeDir(unpackedJarDir);
      }

      return result;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Futures.immediateFailedFuture(t);
    }
  }

  private void removeDir(File dir) {
    try {
      DirUtils.deleteDirectoryContents(dir);
    } catch (IOException e) {
      LOG.warn("Failed to delete directory {}", dir, e);
    }
  }
}
