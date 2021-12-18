/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.app.DefaultApplicationContext;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.lang.ClassLoaders;
import io.cdap.cdap.common.lang.CombineClassLoader;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.internal.app.deploy.pipeline.AppDeploymentInfo;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;

/**
 * In Memory Configurator doesn't spawn a external process, but does this in memory.
 */
public final class InMemoryConfigurator implements Configurator {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryConfigurator.class);

  private final CConfiguration cConf;
  private final String applicationName;
  private final String applicationVersion;
  private final String configString;
  private final File baseUnpackDir;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final Id.Namespace appNamespace;

  private final PluginFinder pluginFinder;
  private final String appClassName;
  private final Id.Artifact artifactId;

  // These fields are needed to create the classLoader in the config method
  private final ArtifactRepository artifactRepository;
  private final Location artifactLocation;
  private final Impersonator impersonator;

  @Inject
  public InMemoryConfigurator(CConfiguration cConf, PluginFinder pluginFinder, Impersonator impersonator,
                              ArtifactRepository artifactRepository, @Assisted AppDeploymentInfo deploymentInfo) {
    this.cConf = cConf;
    this.pluginFinder = pluginFinder;
    this.appNamespace = Id.Namespace.fromEntityId(deploymentInfo.getNamespaceId());
    this.artifactId = Id.Artifact.fromEntityId(deploymentInfo.getArtifactId());
    this.appClassName = deploymentInfo.getApplicationClass().getClassName();
    this.applicationName = deploymentInfo.getApplicationName();
    this.applicationVersion = deploymentInfo.getApplicationVersion();
    this.configString = deploymentInfo.getConfigString() == null ? "" : deploymentInfo.getConfigString();
    this.baseUnpackDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                  cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();

    this.impersonator = impersonator;
    this.artifactRepository = artifactRepository;
    this.artifactLocation = deploymentInfo.getArtifactLocation();
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

    // Create the classloader
    EntityImpersonator classLoaderImpersonator = new EntityImpersonator(artifactId.toEntityId(), impersonator);
    try (CloseableClassLoader classLoader =
           artifactRepository.createArtifactClassLoader(new ArtifactDescriptor(artifactId.getNamespace().getId(),
                                                                               artifactId.toArtifactId(),
                                                                               artifactLocation),
                                                        classLoaderImpersonator)) {
      SettableFuture<ConfigResponse> result = SettableFuture.create();

      Object appMain = classLoader.loadClass(appClassName).newInstance();
      if (!(appMain instanceof Application)) {
        throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
                                                      appMain.getClass().getName()));
      }

      Application<?> app = (Application<?>) appMain;
      ConfigResponse response = createResponse(app, classLoader);
      result.set(response);

      return result;

    } catch (Throwable t) {
      return Futures.immediateFailedFuture(t);
    }
  }

  private <T extends Config> ConfigResponse createResponse(Application<T> app,
                                                           ClassLoader artifactClassLoader) throws Exception {
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
                                            configString, pluginFinder, pluginInstantiator);
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

      try {
        ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(
          new CombineClassLoader(null, app.getClass().getClassLoader(), getClass().getClassLoader()));
        try {
          app.configure(configurer, new DefaultApplicationContext<>(appConfig));
        } finally {
          ClassLoaders.setContextClassLoader(oldClassLoader);
        }
      } catch (Throwable t) {
        Throwable rootCause = Throwables.getRootCause(t);
        if (rootCause instanceof ClassNotFoundException) {
          // Heuristic to provide better error message
          String missingClass = rootCause.getMessage();

          // If the missing class has "spark" in the name, try to see if Spark is available
          if (missingClass.startsWith("org.apache.spark.") || missingClass.startsWith("io.cdap.cdap.api.spark.")) {
            // Try to load the SparkContext class, which should be available if Spark is available in the platform
            try {
              artifactClassLoader.loadClass("org.apache.spark.SparkContext");
            } catch (ClassNotFoundException e) {
              // Spark is not available, it is most likely caused by missing Spark in the platform
              throw new IllegalStateException(
                "Missing Spark related class " + missingClass +
                  ". It may be caused by unavailability of Spark. " +
                  "Please verify environment variable " + Constants.SPARK_HOME + " is set correctly", t);
            }

            // Spark is available, can be caused by incompatible Spark version
            throw new InvalidArtifactException(
              "Missing Spark related class " + missingClass +
                ". Configured to use Spark located at " + System.getenv(Constants.SPARK_HOME) +
                ", which may be incompatible with the one required by the application", t);
          }
          // If Spark is available or the missing class is not a spark related class,
          // then the missing class is most likely due to some missing library in the artifact jar
          throw new InvalidArtifactException(
            "Missing class " + missingClass +
              ". It may be caused by missing dependency jar(s) in the artifact jar.", t);
        }
        throw t;
      }
    } finally {
      try {
        DirUtils.deleteDirectoryContents(tempDir);
      } catch (IOException e) {
        LOG.warn("Exception raised when deleting directory {}", tempDir, e);
      }
    }
    ApplicationSpecification specification = configurer.createSpecification(applicationName, applicationVersion);
    AppSpecInfo appSpecInfo = new AppSpecInfo(specification, configurer.getSystemTables(), configurer.getMetadata());
    return new DefaultConfigResponse(0, appSpecInfo);
  }
}
