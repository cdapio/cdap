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

package co.cask.cdap.test;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.plugins.PluginPropertyField;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.program.ManifestFields;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.StickyEndpointStrategy;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.internal.AppFabricClient;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.internal.test.PluginJarHelper;
import co.cask.cdap.proto.AdapterConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.DefaultAdapterManager;
import co.cask.cdap.test.internal.StreamManagerFactory;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 *
 */
public class UnitTestManager implements TestManager {

  private static final Gson GSON = new Gson();
  private final AppFabricClient appFabricClient;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryClient;
  private final ApplicationManagerFactory appManagerFactory;
  private final NamespaceAdmin namespaceAdmin;
  private final StreamManagerFactory streamManagerFactory;
  private final LocationFactory locationFactory;
  private final File templateDir;
  private final File pluginDir;

  @Inject
  public UnitTestManager(AppFabricClient appFabricClient,
                         DatasetFramework datasetFramework,
                         TransactionSystemClient txSystemClient,
                         DiscoveryServiceClient discoveryClient,
                         ApplicationManagerFactory appManagerFactory,
                         NamespaceAdmin namespaceAdmin,
                         StreamManagerFactory streamManagerFactory,
                         LocationFactory locationFactory,
                         CConfiguration cConf) {
    this.appFabricClient = appFabricClient;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryClient = discoveryClient;
    this.appManagerFactory = appManagerFactory;
    this.namespaceAdmin = namespaceAdmin;
    this.streamManagerFactory = streamManagerFactory;
    this.locationFactory = locationFactory;
    // this should have been set to a temp dir during injector setup
    this.templateDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_DIR));
    this.pluginDir = new File(cConf.get(Constants.AppFabric.APP_TEMPLATE_PLUGIN_DIR));
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * other programs defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace,
                                              Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    return deployApplication(namespace, applicationClz, null, bundleEmbeddedJars);
  }

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace, Class<? extends Application> applicationClz,
                                              @Nullable Config configObject, File... bundleEmbeddedJars) {
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");
    String appConfig = "";
    TypeToken typeToken = TypeToken.of(applicationClz);
    TypeToken<?> configToken = typeToken.resolveType(Application.class.getTypeParameters()[0]);

    try {
      if (configObject != null) {
        appConfig = GSON.toJson(configObject);
      } else {
        configObject = (Config) configToken.getRawType().newInstance();
      }

      Application app = applicationClz.newInstance();
      DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
      app.configure(configurer, new DefaultApplicationContext(configObject));
      ApplicationSpecification appSpec = configurer.createSpecification();
      Location deployedJar = appFabricClient.deployApplication(namespace, appSpec.getName(),
                                                               applicationClz, appConfig, bundleEmbeddedJars);
      return appManagerFactory.create(Id.Application.from(namespace, appSpec.getName()), deployedJar, appSpec);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deployTemplate(Id.Namespace namespace, Id.ApplicationTemplate templateId,
                             Class<? extends ApplicationTemplate> templateClz,
                             String... exportPackages) throws IOException {
    Manifest manifest = new Manifest();
    StringBuilder packagesStr = new StringBuilder().append(templateClz.getPackage().getName());
    for (String exportPackage : exportPackages) {
      packagesStr.append(",");
      packagesStr.append(exportPackage);
    }
    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, packagesStr.toString());
    Location adapterJar = AppJarHelper.createDeploymentJar(locationFactory, templateClz, manifest);
    File destination =  new File(String.format("%s/%s", templateDir.getAbsolutePath(), adapterJar.getName()));
    Files.copy(Locations.newInputSupplier(adapterJar), destination);
    appFabricClient.deployTemplate(namespace, templateId);
  }

  @Override
  public void addTemplatePlugins(Id.ApplicationTemplate templateId, String jarName,
                                 Class<?> pluginClz, Class<?>... classes) throws IOException {
    Manifest manifest = new Manifest();
    Set<String> exportPackages = new HashSet<>();
    for (Class<?> cls : Iterables.concat(ImmutableList.of(pluginClz), ImmutableList.copyOf(classes))) {
      exportPackages.add(cls.getPackage().getName());
    }

    manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, Joiner.on(',').join(exportPackages));
    Location pluginJar = PluginJarHelper.createPluginJar(locationFactory, manifest, pluginClz, classes);
    File templatePluginDir = new File(pluginDir, templateId.getId());
    DirUtils.mkdirs(templatePluginDir);
    File destination = new File(templatePluginDir, jarName);
    Files.copy(Locations.newInputSupplier(pluginJar), destination);
  }

  @Override
  public void addTemplatePluginJson(Id.ApplicationTemplate templateId, String fileName, String type, String name,
                                    String description, String className,
                                    PluginPropertyField... fields) throws IOException {

    File templateDir = new File(pluginDir, templateId.getId());
    DirUtils.mkdirs(templateDir);

    JsonObject json = new JsonObject();
    json.addProperty("type", type);
    json.addProperty("name", name);
    json.addProperty("description", description);
    json.addProperty("className", className);
    if (fields.length > 0) {
      Map<String, PluginPropertyField> properties = Maps.newHashMap();
      for (PluginPropertyField field : fields) {
        properties.put(field.getName(), field);
      }
      json.add("properties", GSON.toJsonTree(properties));
    }
    File configFile = new File(templateDir, fileName);
    try (Writer writer = Files.newWriter(configFile, Charsets.UTF_8)) {
      GSON.toJson(Lists.newArrayList(json), writer);
    }
  }

  @Override
  public AdapterManager createAdapter(Id.Adapter adapterId, AdapterConfig config) {
    appFabricClient.createAdapter(adapterId, config);
    return new DefaultAdapterManager(appFabricClient, adapterId);
  }

  @Override
  public void clear() throws Exception {
    try {
      appFabricClient.reset();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      RuntimeStats.resetAll();
    }
  }

  @Beta
  @Override
  public final void deployDatasetModule(Id.Namespace namespace,
                                        String moduleName, Class<? extends DatasetModule> datasetModule)
    throws Exception {
    datasetFramework.addModule(Id.DatasetModule.from(namespace, moduleName), datasetModule.newInstance());
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                             String datasetTypeName, String datasetInstanceName,
                                                             DatasetProperties props) throws Exception {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetInstanceName);
    datasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
    return datasetFramework.getAdmin(datasetInstanceId, null);
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(Id.Namespace namespace,
                                                             String datasetTypeName,
                                                             String datasetInstanceName) throws Exception {
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetInstanceName);
    datasetFramework.addInstance(datasetTypeName, datasetInstanceId, DatasetProperties.EMPTY);
    return datasetFramework.getAdmin(datasetInstanceId, null);
  }

  /**
   * Gets Dataset manager of Dataset instance of type <T>
   * @param datasetInstanceName - instance name of dataset
   * @return Dataset Manager of Dataset instance of type <T>
   * @throws Exception
   */
  @Beta
  @Override
  public final <T> DataSetManager<T> getDataset(Id.Namespace namespace, String datasetInstanceName) throws Exception {
    //TODO: Expose namespaces later. Hardcoding to default right now.
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(namespace, datasetInstanceName);
    @SuppressWarnings("unchecked")
    final T dataSet = (T) datasetFramework.getDataset(datasetInstanceId, new HashMap<String, String>(), null);
    try {
      final TransactionContext txContext;
      // not every dataset is TransactionAware. FileSets for example, are not transactional.
      if (dataSet instanceof TransactionAware) {
        TransactionAware txAwareDataset = (TransactionAware) dataSet;
        txContext = new TransactionContext(txSystemClient, Lists.newArrayList(txAwareDataset));
        txContext.start();
      } else {
        txContext = null;
      }
      return new DataSetManager<T>() {
        @Override
        public T get() {
          return dataSet;
        }

        @Override
        public void flush() {
          try {
            if (txContext != null) {
              txContext.finish();
              txContext.start();
            }
          } catch (TransactionFailureException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Returns a JDBC connection that allows to run SQL queries over data sets.
   */
  @Beta
  @Override
  public final Connection getQueryClient(Id.Namespace namespace) throws Exception {
    // this makes sure the Explore JDBC driver is loaded
    Class.forName(ExploreDriver.class.getName());

    Discoverable discoverable = new StickyEndpointStrategy(
      discoveryClient.discover(Constants.Service.EXPLORE_HTTP_USER_SERVICE)).pick();

    if (null == discoverable) {
      throw new IOException("Explore service could not be discovered.");
    }

    InetSocketAddress address = discoverable.getSocketAddress();
    String host = address.getHostName();
    int port = address.getPort();

    String connectString = String.format("%s%s:%d?namespace=%s", Constants.Explore.Jdbc.URL_PREFIX, host, port,
                                         namespace.getId());

    return DriverManager.getConnection(connectString);
  }

  @Override
  public void createNamespace(NamespaceMeta namespaceMeta) throws Exception {
    namespaceAdmin.createNamespace(namespaceMeta);
  }

  @Override
  public void deleteNamespace(Id.Namespace namespace) throws Exception {
    namespaceAdmin.deleteNamespace(namespace);
  }

  @Override
  public StreamManager getStreamManager(Id.Stream streamId) {
    return streamManagerFactory.create(streamId);
  }
}
