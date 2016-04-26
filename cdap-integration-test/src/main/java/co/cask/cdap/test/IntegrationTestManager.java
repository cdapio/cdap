/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.test;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.MockAppConfigurer;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.client.DatasetModuleClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ProgramResources;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.internal.app.runtime.artifact.Artifacts;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.DatasetInstanceConfiguration;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.remote.RemoteApplicationManager;
import co.cask.cdap.test.remote.RemoteArtifactManager;
import co.cask.cdap.test.remote.RemoteStreamManager;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * {@link TestManager} for integration tests.
 */
public class IntegrationTestManager implements TestManager {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestManager.class);
  private static final Gson GSON = new Gson();
  private static final ClassAcceptor CLASS_ACCEPTOR = new ClassAcceptor() {
    final Set<String> visibleResources = ProgramResources.getVisibleResources();

    @Override
    public boolean accept(String className, URL classUrl, URL classPathUrl) {
      String resourceName = className.replace('.', '/') + ".class";
      if (visibleResources.contains(resourceName)) {
        return false;
      }
      // Always includes Scala class. It is for CDAP-5168
      if (resourceName.startsWith("scala/")) {
        return true;
      }
      // If it is loading by spark framework, don't include it in the app JAR
      return !SparkRuntimeUtils.SPARK_PROGRAM_CLASS_LOADER_FILTER.acceptResource(resourceName);
    }
  };

  private final ApplicationClient applicationClient;
  private final ArtifactClient artifactClient;
  private final DatasetClient datasetClient;
  private final DatasetModuleClient datasetModuleClient;
  private final NamespaceClient namespaceClient;
  private final ProgramClient programClient;

  private final ClientConfig clientConfig;
  private final RESTClient restClient;

  private final LocationFactory locationFactory;
  private final File tmpFolder;

  public IntegrationTestManager(ClientConfig clientConfig, RESTClient restClient, File tmpFolder) {
    this.clientConfig = clientConfig;
    this.restClient = restClient;
    this.tmpFolder = tmpFolder;
    this.locationFactory = new LocalLocationFactory(tmpFolder);

    this.applicationClient = new ApplicationClient(clientConfig, restClient);
    this.artifactClient = new ArtifactClient(clientConfig, restClient);
    this.datasetClient = new DatasetClient(clientConfig, restClient);
    this.datasetModuleClient = new DatasetModuleClient(clientConfig, restClient);
    this.namespaceClient = new NamespaceClient(clientConfig, restClient);
    this.programClient = new ProgramClient(clientConfig, restClient);
  }

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace,
                                              Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    // See if the application class comes from file or jar.
    // If it's from JAR, no need to trace dependency since it should already be in an application jar.
    URL appClassURL = applicationClz.getClassLoader()
                                    .getResource(applicationClz.getName().replace('.', '/') + ".class");
    // Should never happen, otherwise the ClassLoader is broken
    Preconditions.checkNotNull(appClassURL, "Cannot find class %s from the classloader", applicationClz);

    String appConfig = "";
    TypeToken typeToken = TypeToken.of(applicationClz);
    Type configType = Artifacts.getConfigType(applicationClz);

    try {
      if (configObject != null) {
        appConfig = GSON.toJson(configObject);
      } else {
        configObject = (Config) TypeToken.of(configType).getRawType().newInstance();
      }

      // Create and deploy application jar
      File appJarFile = new File(tmpFolder, String.format("%s-1.0.0-SNAPSHOT.jar", applicationClz.getSimpleName()));
      try {
        if ("jar".equals(appClassURL.getProtocol())) {
          copyJarFile(appClassURL, appJarFile);
        } else {
          Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClz, new Manifest(),
                                                             CLASS_ACCEPTOR, bundleEmbeddedJars);
          Files.copy(Locations.newInputSupplier(appJar), appJarFile);
        }
        applicationClient.deploy(namespace, appJarFile, appConfig);
      } finally {
        if (!appJarFile.delete()) {
          LOG.warn("Failed to delete temporary app jar {}", appJarFile.getAbsolutePath());
        }
      }

      // Extracts application id from the application class
      Application application = applicationClz.newInstance();
      MockAppConfigurer configurer = new MockAppConfigurer(application);
      application.configure(configurer, new DefaultApplicationContext<>(configObject));
      String applicationId = configurer.getName();
      return new RemoteApplicationManager(Id.Application.from(namespace, applicationId), clientConfig, restClient);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationManager deployApplication(Id.Application appId,
                                              AppRequest appRequest) throws Exception {
    applicationClient.deploy(appId, appRequest);
    return new RemoteApplicationManager(appId, clientConfig, restClient);
  }

  @Override
  public ApplicationManager getApplicationManager(ApplicationId applicationId) {
    return new RemoteApplicationManager(applicationId.toId(), clientConfig, restClient);
  }

  @Override
  public void addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    artifactClient.add(artifactId, null, Files.newInputStreamSupplier(artifactFile));
  }

  @Override
  public ArtifactManager addArtifact(ArtifactId artifactId, final File artifactFile) throws Exception {
    artifactClient.add(artifactId.toId(), null, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return new FileInputStream(artifactFile);
      }
    });
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId, appClass, new Manifest());
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, String... exportPackages) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, exportPackages);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        String... exportPackages) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    addAppArtifact(artifactId, appClass, manifest);
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception {
    addAppArtifact(artifactId.toEntityId(), appClass, manifest);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest) throws Exception {
    final Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest, CLASS_ACCEPTOR);

    artifactClient.add(artifactId.toId(), null, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJar.getInputStream();
      }
    });
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      Ids.namespace(parent.getNamespace()).toId(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, pluginClass, pluginClasses);
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parents, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(artifactId.toId(), parents, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJar.getInputStream();
      }
    });
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parent.toEntityId(), additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      Ids.namespace(parent.getNamespace()).toId(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, additionalPlugins, pluginClass, pluginClasses);
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    addPluginArtifact(artifactId.toEntityId(), parents, additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(
      Ids.namespace(artifactId.getNamespace()).toId(),
      artifactId.getArtifact(),
      new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return appJar.getInputStream();
        }
      },
      artifactId.getVersion(),
      parents,
      additionalPlugins
    );
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void deleteArtifact(Id.Artifact artifactId) throws Exception {
    artifactClient.delete(artifactId);
  }

  @Override
  public void clear() throws Exception {
    for (NamespaceMeta namespace : namespaceClient.list()) {
      programClient.stopAll(Id.Namespace.from(namespace.getName()));
    }
    namespaceClient.deleteAll();
  }

  @Override
  public void deployDatasetModule(Id.Namespace namespace, String moduleName,
                                  Class<? extends DatasetModule> datasetModule) throws Exception {
    datasetModuleClient.add(Id.DatasetModule.from(namespace, moduleName),
                            datasetModule.getName(),
                            createModuleJarFile(datasetModule));
  }

  private File createModuleJarFile(Class<?> cls) throws IOException {
    String version = String.format("1.0.%d-SNAPSHOT", System.currentTimeMillis());
    File moduleJarFile = new File(tmpFolder, String.format("%s-%s.jar", cls.getSimpleName(), version));
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest(), CLASS_ACCEPTOR);
    Files.copy(Locations.newInputSupplier(deploymentJar), moduleJarFile);
    return moduleJarFile;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName, String datasetInstanceName,
                                                       DatasetProperties props) throws Exception {
    Id.DatasetInstance datasetInstance = Id.DatasetInstance.from(namespace, datasetInstanceName);
    DatasetInstanceConfiguration dsConf = new DatasetInstanceConfiguration(datasetTypeName, props.getProperties(),
                                                                           props.getDescription());

    datasetClient.create(datasetInstance, dsConf);
    return (T) new RemoteDatasetAdmin(datasetClient, datasetInstance, dsConf);
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName,
                                                       String datasetInstanceName) throws Exception {
    return addDatasetInstance(namespace, datasetTypeName, datasetInstanceName, DatasetProperties.EMPTY);
  }

  @Override
  public <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getQueryClient(Id.Namespace namespace) throws Exception {
    ConnectionConfig connConfig = clientConfig.getConnectionConfig();
    String url = String.format("%s%s:%d?namespace=%s", Constants.Explore.Jdbc.URL_PREFIX, connConfig.getHostname(),
                               connConfig.getPort(), namespace.getId());
    return new ExploreDriver().connect(url, new Properties());
  }

  @Override
  public void createNamespace(NamespaceMeta namespaceMeta) throws Exception {
    namespaceClient.create(namespaceMeta);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespace) throws Exception {
    namespaceClient.delete(namespace);
  }

  @Override
  public StreamManager getStreamManager(Id.Stream streamId) {
    return new RemoteStreamManager(clientConfig, restClient, streamId);
  }

  @Override
  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    applicationClient.deleteAll(namespaceId.toId());
  }

  /**
   * Copies the jar content to a local file
   *
   * @param jarURL URL representing the jar location or an entry in the jar. An entry URL has format of
   *               {@code jar:[jarURL]!/path/to/entry}
   * @param file the local file to copy to
   */
  private void copyJarFile(URL jarURL, File file) {
    try {
      JarURLConnection jarConn = (JarURLConnection) jarURL.openConnection();
      try {
        Files.copy(Resources.newInputStreamSupplier(jarConn.getJarFileURL()), file);
      } finally {
        jarConn.getJarFile().close();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Manifest createManifest(Class<?> cls, Class<?>... classes) {
    Manifest manifest = new Manifest();
    Set<String> exportPackages = new HashSet<>();
    exportPackages.add(cls.getPackage().getName());
    for (Class<?> clz : classes) {
      exportPackages.add(clz.getPackage().getName());
    }

    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    return manifest;
  }
}
