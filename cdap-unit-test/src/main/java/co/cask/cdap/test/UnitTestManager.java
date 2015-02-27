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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationContext;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.StickyEndpointStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.explore.jdbc.ExploreDriver;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.internal.AppFabricClient;
import co.cask.cdap.test.internal.ApplicationManagerFactory;
import co.cask.cdap.test.internal.DefaultId;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;

/**
 *
 */
public class UnitTestManager implements TestManager {

  private final Injector injector;
  private final AppFabricClient appFabricClient;
  private final DatasetFramework datasetFramework;
  private final TransactionSystemClient txSystemClient;
  private final DiscoveryServiceClient discoveryClient;

  public UnitTestManager(Injector injector, AppFabricClient appFabricClient, DatasetFramework datasetFramework,
                         TransactionSystemClient txSystemClient, DiscoveryServiceClient discoveryClient) {
    this.injector = injector;
    this.appFabricClient = appFabricClient;
    this.datasetFramework = datasetFramework;
    this.txSystemClient = txSystemClient;
    this.discoveryClient = discoveryClient;
  }

  /**
   * Deploys an {@link Application}. The {@link co.cask.cdap.api.flow.Flow Flows} and
   * {@link co.cask.cdap.api.procedure.Procedure Procedures} defined in the application
   * must be in the same or children package as the application.
   *
   * @param applicationClz The application class
   * @return An {@link co.cask.cdap.test.ApplicationManager} to manage the deployed application.
   */
  @Override
  public ApplicationManager deployApplication(Class<? extends Application> applicationClz,
                                              File... bundleEmbeddedJars) {
    Preconditions.checkNotNull(applicationClz, "Application class cannot be null.");

    try {
      Application app = applicationClz.newInstance();
      DefaultAppConfigurer configurer = new DefaultAppConfigurer(app);
      app.configure(configurer, new ApplicationContext());
      ApplicationSpecification appSpec = configurer.createSpecification();

      Location deployedJar = appFabricClient.deployApplication(appSpec.getName(), applicationClz, bundleEmbeddedJars);
      ApplicationManager manager = injector.getInstance(ApplicationManagerFactory.class)
        .create(DefaultId.NAMESPACE.getId(), appSpec.getName(),
                deployedJar, appSpec);
      return manager;

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
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
  public final void deployDatasetModule(String moduleName, Class<? extends DatasetModule> datasetModule)
    throws Exception {
    //TODO: Expose namespaces later. Hardcoding to default right now.
    datasetFramework.addModule(Id.DatasetModule.from(DefaultId.NAMESPACE, moduleName), datasetModule.newInstance());
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName, String datasetInstanceName,
                                                             DatasetProperties props) throws Exception {
    //TODO: Expose namespaces later. Hardcoding to default right now.
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(DefaultId.NAMESPACE, datasetInstanceName);
    datasetFramework.addInstance(datasetTypeName, datasetInstanceId, props);
    return datasetFramework.getAdmin(datasetInstanceId, null);
  }

  @Beta
  @Override
  public final <T extends DatasetAdmin> T addDatasetInstance(String datasetTypeName,
                                                             String datasetInstanceName) throws Exception {
    //TODO: Expose namespaces later. Hardcoding to default right now.
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(DefaultId.NAMESPACE, datasetInstanceName);
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
  public final <T> DataSetManager<T> getDataset(String datasetInstanceName) throws Exception {
    //TODO: Expose namespaces later. Hardcoding to default right now.
    Id.DatasetInstance datasetInstanceId = Id.DatasetInstance.from(DefaultId.NAMESPACE, datasetInstanceName);
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
  public final Connection getQueryClient() throws Exception {

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
                                         Constants.DEFAULT_NAMESPACE);

    return DriverManager.getConnection(connectString);
  }

}
