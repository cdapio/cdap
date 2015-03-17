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

import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.MetaClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.remote.RemoteApplicationManager;
import com.google.common.base.Throwables;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;

/**
 *
 */
public class IntegrationTestManager implements TestManager {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestManager.class);

  private final MetaClient metaClient;
  private final ApplicationClient applicationClient;
  private final ClientConfig clientConfig;
  private final LocationFactory locationFactory;
  private final ProgramClient programClient;

  public IntegrationTestManager(ClientConfig clientConfig, LocationFactory locationFactory) {
    this.clientConfig = clientConfig;
    this.locationFactory = locationFactory;
    this.metaClient = new MetaClient(clientConfig);
    this.applicationClient = new ApplicationClient(clientConfig);
    this.programClient = new ProgramClient(clientConfig);
  }

  @Override
  public ApplicationManager deployApplication(Id.Namespace namespace,
                                              Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    File appJarFile = null;
    try {
      appJarFile = createAppJarFile(applicationClz, bundleEmbeddedJars);
      applicationClient.deploy(appJarFile);

      Application application = applicationClz.newInstance();
      DefaultAppConfigurer configurer = new DefaultAppConfigurer(application);
      application.configure(configurer, new ApplicationContext());
      String applicationId = configurer.createSpecification().getName();
      return new RemoteApplicationManager(Id.Application.from(namespace, applicationId), clientConfig);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      if (appJarFile != null && !appJarFile.delete()) {
        LOG.warn("Failed to delete temporary app jar {}", appJarFile.getAbsolutePath());
      }
    }
  }

  @Override
  public void clear() throws Exception {
    programClient.stopAll();
    metaClient.resetUnrecoverably();
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

  private File createAppJarFile(Class<?> cls, File[] bundleEmbeddedJars) throws IOException {
    return AppFabricClient.createDeploymentJar(locationFactory, cls, bundleEmbeddedJars);
  }
}
