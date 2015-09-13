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

package co.cask.cdap.test;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.MockAppConfigurer;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ArtifactClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.test.remote.RemoteApplicationManager;
import co.cask.cdap.test.remote.RemoteStreamManager;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 *
 */
public class IntegrationTestManager implements TestManager {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestManager.class);
  private static final Gson GSON = new Gson();

  private final ArtifactClient artifactClient;
  private final ApplicationClient applicationClient;
  private final ClientConfig clientConfig;
  private final RESTClient restClient;
  private final LocationFactory locationFactory;
  private final ProgramClient programClient;
  private final NamespaceClient namespaceClient;
  private final File tmpFolder;

  public IntegrationTestManager(ClientConfig clientConfig, RESTClient restClient, File tmpFolder) {
    this.clientConfig = clientConfig;
    this.restClient = restClient;
    this.tmpFolder = tmpFolder;
    this.locationFactory = new LocalLocationFactory(tmpFolder);
    this.applicationClient = new ApplicationClient(clientConfig, restClient);
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.namespaceClient = new NamespaceClient(clientConfig, restClient);
    this.artifactClient = new ArtifactClient(clientConfig, restClient);
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
    TypeToken<?> configToken = typeToken.resolveType(Application.class.getTypeParameters()[0]);

    try {
      if (configObject != null) {
        appConfig = GSON.toJson(configObject);
      } else {
        configObject = ((Class<Config>) configToken.getRawType()).newInstance();
      }

      // Create and deploy application jar
      File appJarFile = new File(tmpFolder, String.format("%s-1.0.0-SNAPSHOT.jar", applicationClz.getSimpleName()));
      try {
        if ("jar".equals(appClassURL.getProtocol())) {
          copyJarFile(appClassURL, appJarFile);
        } else {
          Location appJar = AppJarHelper.createDeploymentJar(locationFactory, applicationClz, bundleEmbeddedJars);
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
  public void addArtifact(Id.Artifact artifactId, File artifactFile) throws Exception {
    artifactClient.add(artifactId, null, Files.newInputStreamSupplier(artifactFile));
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass) throws Exception {
    addAppArtifact(artifactId, appClass, new Manifest());
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, String... exportPackages) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    addAppArtifact(artifactId, appClass, manifest);
  }

  @Override
  public void addAppArtifact(Id.Artifact artifactId, Class<?> appClass, Manifest manifest) throws Exception {
    final Location appJar = AppJarHelper.createDeploymentJar(locationFactory, appClass, manifest);

    artifactClient.add(artifactId, null, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJar.getInputStream();
      }
    });
    appJar.delete();
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getNamespace(), parent.getName(), parent.getVersion(), true, parent.getVersion(), true));
    addPluginArtifact(artifactId, parents, pluginClass, pluginClasses);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(artifactId, parents, new InputSupplier<InputStream>() {
      @Override
      public InputStream getInput() throws IOException {
        return appJar.getInputStream();
      }
    });
    appJar.delete();
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Id.Artifact parent,
                                Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass,
                                Class<?>... pluginClasses) throws Exception {
    Set<ArtifactRange> parents = new HashSet<>();
    parents.add(new ArtifactRange(
      parent.getNamespace(), parent.getName(), parent.getVersion(), true, parent.getVersion(), true));
    addPluginArtifact(artifactId, parents, additionalPlugins, pluginClass, pluginClasses);
  }

  @Override
  public void addPluginArtifact(Id.Artifact artifactId, Set<ArtifactRange> parents,
                                @Nullable Set<PluginClass> additionalPlugins,
                                Class<?> pluginClass, Class<?>... pluginClasses) throws Exception {
    Manifest manifest = createManifest(pluginClass, pluginClasses);
    final Location appJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClass, pluginClasses);
    artifactClient.add(
      artifactId.getNamespace(),
      artifactId.getName(),
      new InputSupplier<InputStream>() {
        @Override
        public InputStream getInput() throws IOException {
          return appJar.getInputStream();
        }
      },
      artifactId.getVersion().getVersion(),
      parents,
      additionalPlugins
    );
    appJar.delete();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName, String datasetInstanceName,
                                                       DatasetProperties props) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                       String datasetTypeName,
                                                       String datasetInstanceName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Connection getQueryClient(Id.Namespace namespace) throws Exception {
    throw new UnsupportedOperationException();
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
