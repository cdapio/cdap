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
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.client.AdapterClient;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.util.RESTClient;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.remote.RemoteAdapterManager;
import co.cask.cdap.test.remote.RemoteApplicationManager;
import co.cask.cdap.test.remote.RemoteStreamManager;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.sql.Connection;
import javax.annotation.Nullable;

/**
 *
 */
public class IntegrationTestManager implements TestManager {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestManager.class);
  private static final Gson GSON = new Gson();

  private final ApplicationClient applicationClient;
  private final ClientConfig clientConfig;
  private final RESTClient restClient;
  private final LocationFactory locationFactory;
  private final ProgramClient programClient;
  private final NamespaceClient namespaceClient;
  private final AdapterClient adapterClient;

  public IntegrationTestManager(ClientConfig clientConfig, RESTClient restClient, LocationFactory locationFactory) {
    this.clientConfig = clientConfig;
    this.restClient = restClient;
    this.locationFactory = locationFactory;
    this.applicationClient = new ApplicationClient(clientConfig, restClient);
    this.programClient = new ProgramClient(clientConfig, restClient);
    this.namespaceClient = new NamespaceClient(clientConfig, restClient);
    this.adapterClient = new AdapterClient(clientConfig, restClient);
  }

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace,
                                              Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
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
        configObject = (Config) configToken.getRawType().newInstance();
      }

      // Create and deploy application jar
      File appJarFile = File.createTempFile(applicationClz.getSimpleName(), ".jar");
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
      DefaultAppConfigurer configurer = new DefaultAppConfigurer(application);
      application.configure(configurer, new DefaultApplicationContext(configObject));
      String applicationId = configurer.createSpecification().getName();
      return new RemoteApplicationManager(Id.Application.from(namespace, applicationId), clientConfig, restClient);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deployTemplate(Id.Namespace namespace, Id.ApplicationTemplate templateId,
                             Class<? extends ApplicationTemplate> templateClz,
                             String... exportPackages) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTemplatePlugins(Id.ApplicationTemplate templateId, String jarName,
                                 Class<?> pluginClz, Class<?>... classes) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTemplatePluginJson(Id.ApplicationTemplate templateId, String fileName, String type, String name,
                                    String description, String className,
                                    PluginPropertyField... fields) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AdapterManager createAdapter(Id.Adapter adapterId, AdapterConfig config) throws Exception {
    adapterClient.create(adapterId, config);
    return new RemoteAdapterManager(adapterId, adapterClient);
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
    namespaceClient.delete(namespace.getId());
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
}
