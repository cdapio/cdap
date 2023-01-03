/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.dataset.DatasetAdmin;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.app.DefaultApplicationContext;
import io.cdap.cdap.app.MockAppConfigurer;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.runtime.spark.SparkResourceFilter;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.client.ArtifactClient;
import io.cdap.cdap.client.DatasetClient;
import io.cdap.cdap.client.DatasetModuleClient;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.util.RESTClient;
import io.cdap.cdap.common.lang.ProgramResources;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.test.PluginJarHelper;
import io.cdap.cdap.internal.app.runtime.artifact.Artifacts;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.DatasetInstanceConfiguration;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.test.remote.RemoteApplicationManager;
import io.cdap.cdap.test.remote.RemoteArtifactManager;
import io.cdap.common.ContentProvider;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * {@link TestManager} for integration tests.
 */
public class IntegrationTestManager extends AbstractTestManager {

  private static final String VERSION = "1.0.0-SNAPSHOT";
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
      return !(new SparkResourceFilter().acceptResource(resourceName));
    }
  };

  private final ApplicationClient applicationClient;
  private final ArtifactClient artifactClient;
  private final DatasetClient datasetClient;
  private final DatasetModuleClient datasetModuleClient;
  private final NamespaceClient namespaceClient;
  private final ProgramClient programClient;
  private final ScheduleClient scheduleClient;

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
    this.scheduleClient = new ScheduleClient(clientConfig, restClient);
  }

  @Override
  @SuppressWarnings("unchecked")
  public ApplicationManager deployApplication(NamespaceId namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    // See if the application class comes from file or jar.
    // If it's from JAR, no need to trace dependency since it should already be in an application jar.
    URL appClassURL = applicationClz.getClassLoader()
      .getResource(applicationClz.getName().replace('.', '/') + ".class");
    // Should never happen, otherwise the ClassLoader is broken
    Preconditions.checkNotNull(appClassURL, "Cannot find class %s from the classloader", applicationClz);

    String appConfig = "";
    Type configType = Artifacts.getConfigType(applicationClz);
    RemoteApplicationManager manager;
    try {
      if (configObject != null) {
        appConfig = GSON.toJson(configObject);
      } else {
        configObject = (Config) TypeToken.of(configType).getRawType().newInstance();
      }

      // Create and deploy application jar
      File appJarFile = new File(tmpFolder, String.format("%s-%s.jar", applicationClz.getSimpleName(), VERSION));
      try {

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(ManifestFields.BUNDLE_VERSION, VERSION);
        manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS, applicationClz.getName());
        if ("jar".equals(appClassURL.getProtocol())) {
          // CDAP-18984 somehow it is possible for a jar with the wrong manifest to be found,
          // so ensure the manifest has the right MAIN_CLASS set
          copyJarFile(appClassURL, appJarFile, manifest);
        } else {
          Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClz, manifest,
                                                             CLASS_ACCEPTOR, bundleEmbeddedJars);

          try (InputStream input = appJar.getInputStream()) {
            Files.copy(input, appJarFile.toPath());
          }
        }

        // Extracts application id from the application class
        Application application = applicationClz.newInstance();
        MockAppConfigurer configurer = new MockAppConfigurer(application);
        application.configure(configurer, new DefaultApplicationContext<>(configObject));
        String applicationId = configurer.getName();

        // Upload artifact for application
        ContentProvider<InputStream> artifactStream = locationFactory.create(appJarFile.toURI())::getInputStream;
        artifactClient.add(namespace, applicationClz.getSimpleName(), artifactStream, VERSION);

        List<ArtifactSummary> deployedArtifacts = artifactClient.list(namespace);
        assert deployedArtifacts.size() > 0;
        // Deploy application
        ArtifactSummary summary = new ArtifactSummary(applicationClz.getSimpleName(), VERSION);
        AppRequest<?> request = new AppRequest<>(summary, appConfig);
        ApplicationId id = namespace.app(applicationId);
        applicationClient.deploy(id, request);

        manager = new RemoteApplicationManager(namespace.app(applicationId), clientConfig, restClient);
      } finally {
        if (!appJarFile.delete()) {
          LOG.warn("Failed to delete temporary app jar {}", appJarFile.getAbsolutePath());
        }
      }
      return manager;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ApplicationManager deployApplication(ApplicationId appId, AppRequest appRequest) throws Exception {
    applicationClient.deploy(appId, appRequest);
    return new RemoteApplicationManager(appId, clientConfig, restClient);
  }

  @Override
  public ApplicationManager getApplicationManager(ApplicationId applicationId) {
    return new RemoteApplicationManager(applicationId, clientConfig, restClient);
  }

  @Override
  public ArtifactManager addArtifact(ArtifactId artifactId, final File artifactFile) throws Exception {
    artifactClient.add(artifactId, null, () -> new FileInputStream(artifactFile));
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId, appClass, new Manifest());
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
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
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest) throws Exception {
    return addAppArtifact(artifactId, appClass, manifest, new File[0]);
  }

  @Override
  public ArtifactManager addAppArtifact(ArtifactId artifactId, Class<?> appClass,
                                        Manifest manifest, File... bundleEmbeddedJars) throws Exception {
    final Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest, CLASS_ACCEPTOR,
                                                             bundleEmbeddedJars);

    artifactClient.add(artifactId, null, appJar::getInputStream);
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getParent().getNamespace(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, pluginClass, pluginClasses);
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(artifactId, parents, appJar::getInputStream);
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, ArtifactId parent,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getParent().getNamespace(), parent.getArtifact(), new ArtifactVersion(parent.getVersion()),
      true, new ArtifactVersion(parent.getVersion()), true));
    addPluginArtifact(artifactId, parents, additionalPlugins, pluginClass, pluginClasses);
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public ArtifactManager addPluginArtifact(ArtifactId artifactId, Set<ArtifactRange> parents,
                                           @Nullable Set<PluginClass> additionalPlugins, Class<?> pluginClass,
                                           Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(
      Ids.namespace(artifactId.getNamespace()),
      artifactId.getArtifact(),
      appJar::getInputStream,
      artifactId.getVersion(),
      parents,
      additionalPlugins
    );
    appJar.delete();
    return new RemoteArtifactManager(clientConfig, restClient, artifactId);
  }

  @Override
  public void clear() throws Exception {
    for (NamespaceMeta namespace : namespaceClient.list()) {
      programClient.stopAll(namespace.getNamespaceId());
    }
    namespaceClient.deleteAll();
  }

  @Override
  public void deployDatasetModule(DatasetModuleId datasetModuleId,
                                  Class<? extends DatasetModule> datasetModule) throws Exception {
    datasetModuleClient.add(datasetModuleId,
                            datasetModule.getName(),
                            createModuleJarFile(datasetModule));
  }

  private File createModuleJarFile(Class<?> cls) throws IOException {
    String version = String.format("1.0.%d-SNAPSHOT", System.currentTimeMillis());
    File moduleJarFile = new File(tmpFolder, String.format("%s-%s.jar", cls.getSimpleName(), version));
    Location deploymentJar = AppJarHelper.createDeploymentJar(locationFactory, cls, new Manifest(), CLASS_ACCEPTOR);
    try (InputStream input = deploymentJar.getInputStream()) {
      Files.copy(input, moduleJarFile.toPath());
    }
    return moduleJarFile;
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(String datasetType, DatasetId datasetId,
                                                       DatasetProperties props) throws Exception {
    DatasetInstanceConfiguration dsConf = new DatasetInstanceConfiguration(datasetType, props.getProperties(),
                                                                           props.getDescription(), null);

    datasetClient.create(datasetId, dsConf);
    return (T) new RemoteDatasetAdmin(datasetClient, datasetId, dsConf);
  }

  @Override
  public void deleteDatasetInstance(DatasetId datasetId) throws Exception {
    datasetClient.delete(datasetId);
  }

  @Override
  public <T> DataSetManager<T> getDataset(DatasetId datasetId) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteAllApplications(NamespaceId namespaceId) throws Exception {
    applicationClient.deleteAll(namespaceId);
  }

  @Override
  public ApplicationDetail getApplicationDetail(ApplicationId applicationId) throws Exception {
    return applicationClient.get(applicationId);
  }

  @Override
  public void addSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    scheduleClient.add(scheduleId, scheduleDetail);
  }

  @Override
  public void updateSchedule(ScheduleId scheduleId, ScheduleDetail scheduleDetail) throws Exception {
    scheduleClient.update(scheduleId, scheduleDetail);
  }

  @Override
  public void deleteSchedule(ScheduleId scheduleId) throws Exception {
    scheduleClient.delete(scheduleId);
  }

  /**
   * Copies the jar content to a local file, optionally adding entries into the Manifest.
   *
   * @param jarURL URL representing the jar location or an entry in the jar. An entry URL has format of {@code
   *   jar:[jarURL]!/path/to/entry}
   * @param file the local file to copy to
   */
  private void copyJarFile(URL jarURL, File file, Manifest manifest) {
    try {
      JarURLConnection jarConn = (JarURLConnection) jarURL.openConnection();
      Manifest man = new Manifest(jarConn.getManifest());
      // add anything extra from the input manifest
      man.getMainAttributes().putAll(manifest.getMainAttributes());

      Set<String> seenEntries = new HashSet<>();
      try (JarOutputStream jarOutputStream = new JarOutputStream(new FileOutputStream(file), man);
           JarInputStream jarInputStream = new JarInputStream(jarConn.getJarFileURL().openStream())) {
        JarEntry jarEntry = jarInputStream.getNextJarEntry();
        while (jarEntry != null) {

          if (seenEntries.add(jarEntry.getName())) {
            jarOutputStream.putNextEntry(jarEntry);
            if (!jarEntry.isDirectory()) {
              ByteStreams.copy(jarInputStream, jarOutputStream);
            }
          }
          jarEntry = jarInputStream.getNextJarEntry();
        }
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
