/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.deploy.ConfigResponse;
import co.cask.cdap.app.deploy.Configurator;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import javax.annotation.Nullable;

/**
 * In Memory Configurator doesn't spawn a external process, but
 * does this in memory.
 *
 * @see SandboxConfigurator
 */
public final class InMemoryConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryConfigurator.class);

  private final CConfiguration cConf;
  private final String applicationName;
  private final String configString;
  private final File baseUnpackDir;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final Id.Namespace appNamespace;

  private final ArtifactRepository artifactRepository;
  private final ClassLoader artifactClassLoader;
  private final String appClassName;
  private final Id.Artifact artifactId;

  public InMemoryConfigurator(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                              String appClassName, ArtifactRepository artifactRepository,
                              ClassLoader artifactClassLoader,
                              @Nullable String applicationName, @Nullable String configString) {
    this.cConf = cConf;
    this.appNamespace = appNamespace;
    this.artifactId = artifactId;
    this.appClassName = appClassName;
    this.applicationName = applicationName;
    this.configString = configString == null ? "" : configString;
    this.artifactRepository = artifactRepository;
    this.artifactClassLoader = artifactClassLoader;
    this.baseUnpackDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                  cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
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
      Object appMain = artifactClassLoader.loadClass(appClassName).newInstance();
      if (!(appMain instanceof Application)) {
        throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
          appMain.getClass().getName()));
      }

      Application app = (Application) appMain;
      ConfigResponse response = createResponse(app);
      result.set(response);

      return result;
    } catch (Throwable t) {
      return Futures.immediateFailedFuture(t);
    }
  }

  private ConfigResponse createResponse(Application app)
    throws InstantiationException, IllegalAccessException, IOException {
    String specJson = getSpecJson(app);
    return new DefaultConfigResponse(0, CharStreams.newReaderSupplier(specJson));
  }

  private <T extends Config> String getSpecJson(Application<T> app) throws IllegalAccessException,
                                                                           InstantiationException, IOException {
    // This Gson cannot be static since it is used to deserialize user class.
    // Gson will keep a static map to class, hence will leak the classloader
    Gson gson = new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();
    // Now, we call configure, which returns application specification.
    DefaultAppConfigurer configurer;

    File tempDir = DirUtils.createTempDir(baseUnpackDir);
    try (
      PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf, app.getClass().getClassLoader(), tempDir)
    ) {
      configurer = new DefaultAppConfigurer(appNamespace, artifactId, app,
                                            configString, artifactRepository, pluginInstantiator);
      T appConfig;
      Type configType = Artifacts.getConfigType(app.getClass());
      if (configString.isEmpty()) {
        //noinspection unchecked
        appConfig = ((Class<T>) configType).newInstance();
      } else {
        try {
          appConfig = gson.fromJson(configString, configType);
        } catch (JsonSyntaxException e) {
          throw new IllegalArgumentException("Invalid JSON configuration was provided. Please check the syntax.", e);
        }
      }

      app.configure(configurer, new DefaultApplicationContext<>(appConfig));
    } finally {
      try {
        DirUtils.deleteDirectoryContents(tempDir);
      } catch (IOException e) {
        LOG.warn("Exception raised when deleting directory {}", tempDir, e);
      }
    }
    ApplicationSpecification specification = configurer.createSpecification(applicationName);

    // Convert the specification to JSON.
    // We write the Application specification to output file in JSON format.
    // TODO: The SchemaGenerator should be injected
    return ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification);
  }
}
