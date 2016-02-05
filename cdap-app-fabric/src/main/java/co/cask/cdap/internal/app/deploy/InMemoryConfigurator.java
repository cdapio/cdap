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
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactClassLoaderFactory;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.app.runtime.artifact.CloseableClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.Id;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

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
  private final Location artifact;
  private final CConfiguration cConf;
  private final String configString;
  private final ArtifactClassLoaderFactory artifactClassLoaderFactory;
  private final File baseUnpackDir;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final Id.Namespace appNamespace;

  private ArtifactRepository artifactRepository;

  // these field provided if going through artifact code path, but not through template code path
  // this is temporary until we can remove templates. (CDAP-2662).
  private String appClassName;
  private Id.Artifact artifactId;

  public InMemoryConfigurator(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                              String appClassName,
                              Location artifact, @Nullable String configString, ArtifactRepository artifactRepository) {
    Preconditions.checkNotNull(artifact);
    this.cConf = cConf;
    this.appNamespace = appNamespace;
    this.artifactId = artifactId;
    this.appClassName = appClassName;
    this.artifact = artifact;
    this.configString = configString;
    this.artifactRepository = artifactRepository;
    this.baseUnpackDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                  cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.artifactClassLoaderFactory = new ArtifactClassLoaderFactory(cConf, baseUnpackDir);
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
      if (appClassName == null) {
        readAppClassName();
      }

      try (CloseableClassLoader artifactClassLoader = artifactClassLoaderFactory.createClassLoader(artifact)) {
        Object appMain = artifactClassLoader.loadClass(appClassName).newInstance();
        if (!(appMain instanceof Application)) {
          throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
            appMain.getClass().getName()));
        }

        Application app = (Application) appMain;
        ConfigResponse response = createResponse(app);
        result.set(response);
      }

      return result;
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      return Futures.immediateFailedFuture(t);
    }
  }

  // remove once app templates are gone
  private void readAppClassName() throws IOException {
    // Load the JAR using the JAR class load and load the manifest file.
    Manifest manifest = BundleJarUtil.getManifest(artifact);
    Preconditions.checkArgument(manifest != null, "Failed to load manifest from %s", artifact);
    Preconditions.checkArgument(manifest.getMainAttributes() != null,
      "Failed to load manifest attributes from %s", artifact);

    appClassName = manifest.getMainAttributes().getValue(ManifestFields.MAIN_CLASS);
    Preconditions.checkArgument(appClassName != null && !appClassName.isEmpty(),
      "Main class attribute cannot be empty");
  }

  private ConfigResponse createResponse(Application app)
    throws InstantiationException, IllegalAccessException, IOException {
    String specJson = getSpecJson(app, configString);
    return new DefaultConfigResponse(0, CharStreams.newReaderSupplier(specJson));
  }

  private String getSpecJson(Application app, final String configString)
    throws IllegalAccessException, InstantiationException, IOException {

    File tempDir = DirUtils.createTempDir(baseUnpackDir);
    // Now, we call configure, which returns application specification.
    DefaultAppConfigurer configurer;
    try (PluginInstantiator pluginInstantiator = new PluginInstantiator(
      cConf, app.getClass().getClassLoader(), tempDir)) {
      configurer =
        new DefaultAppConfigurer(appNamespace, artifactId, app, configString, artifactRepository, pluginInstantiator);

      Config appConfig;
      Type configType = Artifacts.getConfigType(app.getClass());
      if (Strings.isNullOrEmpty(configString)) {
        //noinspection unchecked
        appConfig = ((Class<? extends Config>) configType).newInstance();
      } else {
        try {
          appConfig = new Gson().fromJson(configString, configType);
        } catch (JsonSyntaxException e) {
          throw new IllegalArgumentException("Invalid JSON configuration was provided. Please check the syntax.", e);
        }
      }

      app.configure(configurer, new DefaultApplicationContext(appConfig));
    } finally {
      DirUtils.deleteDirectoryContents(tempDir);
    }
    ApplicationSpecification specification = configurer.createSpecification();

    // Convert the specification to JSON.
    // We write the Application specification to output file in JSON format.
    // TODO: The SchemaGenerator should be injected
    return ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification);
  }
}
