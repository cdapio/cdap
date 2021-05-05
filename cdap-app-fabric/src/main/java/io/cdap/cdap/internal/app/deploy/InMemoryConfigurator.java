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
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationSpecification;
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
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.pipeline.AppSpecInfo;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.internal.app.runtime.artifact.PluginFinder;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.metadata.MetadataValidator;
import io.cdap.cdap.metadata.elastic.ScopedNameOfKindTypeAdapter;
import io.cdap.cdap.metadata.elastic.ScopedNameTypeAdapter;
import io.cdap.cdap.proto.codec.NamespacedEntityIdCodec;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataCodec;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.MetadataMutationCodec;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
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
  private static final Gson GSON =
    ApplicationSpecificationAdapter.addTypeAdapters(new GsonBuilder())
      .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
      .registerTypeAdapter(Metadata.class, new MetadataCodec())
      .registerTypeAdapter(ScopedName.class, new ScopedNameTypeAdapter())
      .registerTypeAdapter(ScopedNameOfKind.class, new ScopedNameOfKindTypeAdapter())
      .registerTypeAdapter(MetadataMutation.class, new MetadataMutationCodec()).create();

  private final CConfiguration cConf;
  private final String applicationName;
  private final String applicationVersion;
  private final String configString;
  private final File baseUnpackDir;
  // this is the namespace that the app will be in, which may be different than the namespace of the artifact.
  // if the artifact is a system artifact, the namespace will be the system namespace.
  private final Id.Namespace appNamespace;

  private final PluginFinder pluginFinder;
  private final ClassLoader artifactClassLoader;
  private final String appClassName;
  private final Id.Artifact artifactId;

  public InMemoryConfigurator(CConfiguration cConf, Id.Namespace appNamespace, Id.Artifact artifactId,
                              String appClassName, PluginFinder pluginFinder,
                              ClassLoader artifactClassLoader,
                              @Nullable String applicationName, @Nullable String applicationVersion,
                              @Nullable String configString) {
    this.cConf = cConf;
    this.appNamespace = appNamespace;
    this.artifactId = artifactId;
    this.appClassName = appClassName;
    this.applicationName = applicationName;
    this.applicationVersion = applicationVersion;
    this.configString = configString == null ? "" : configString;
    this.pluginFinder = pluginFinder;
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

  private <T extends Config> ConfigResponse createResponse(Application<T> app) throws Exception {
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
                                            configString, pluginFinder, pluginInstantiator, applicationName,
                                            new MetadataValidator(cConf));
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

    // Convert the specification to JSON.
    // We write the Application specification to output file in JSON format.
    return new DefaultConfigResponse(0, GSON.toJson(appSpecInfo));
  }
}
