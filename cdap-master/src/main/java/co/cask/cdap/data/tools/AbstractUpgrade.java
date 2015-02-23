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

package co.cask.cdap.data.tools;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.NamespacedDatasetFramework;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseMetricsTableModule;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.internal.app.ApplicationSpecificationAdapter;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.distributed.TransactionService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Abstract class for Upgrade
 */
public abstract class AbstractUpgrade {

  public static final String EMPTY_STRING = "";
  private static final Logger LOG = LoggerFactory.getLogger(AbstractUpgrade.class);
  static final byte[] COLUMN = Bytes.toBytes("c");
  static final String FORWARD_SLASH = "/";
  static final String CDAP_WITH_FORWARD_SLASH = Constants.Logging.SYSTEM_NAME + FORWARD_SLASH;
  static final String DEVELOPER_STRING = "developer";

  static DatasetFramework namespacedFramework;
  static DatasetFramework nonNamespaedFramework;
  static TransactionExecutorFactory executorFactory;
  static CConfiguration cConf;
  static TransactionService txService;
  static ZKClientService zkClientService;
  static LocationFactory locationFactory;
  static TransactionSystemClient txClient;
  static DefaultStore defaultStore;
  static final Gson GSON;

  static {
    GsonBuilder builder = new GsonBuilder();
    ApplicationSpecificationAdapter.addTypeAdapters(builder);
    GSON = builder.create();
  }

  /**
   * Sets up a {@link DatasetFramework} instance for standalone usage.  NOTE: should NOT be used by applications!!!
   */
  public static DatasetFramework createRegisteredDatasetFramework(Injector injector)
    throws DatasetManagementException, IOException {
    CConfiguration cConf = injector.getInstance(CConfiguration.class);

    DatasetDefinitionRegistryFactory registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    DatasetFramework datasetFramework =
      new NamespacedDatasetFramework(new InMemoryDatasetFramework(registryFactory),
                                     new DefaultDatasetNamespace(cConf));
    // TODO: this doesn't sound right. find out why its needed.
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "table"),
                               new HBaseTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "metricsTable"),
                               new HBaseMetricsTableModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "core"), new CoreDatasetsModule());
    datasetFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "fileSet"), new FileSetModule());

    return datasetFramework;
  }

  /**
   * Creates a non-namespaced {@link DatasetFramework} to access existing datasets which are not namespaced
   */
  public DatasetFramework createNonNamespaceDSFramework(Injector injector) throws DatasetManagementException {
    DatasetDefinitionRegistryFactory registryFactory = injector.getInstance(DatasetDefinitionRegistryFactory.class);
    DatasetFramework nonNamespacedFramework = new InMemoryDatasetFramework(registryFactory);
    nonNamespacedFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "table"),
                                     new HBaseTableModule());
    nonNamespacedFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "metricsTable"),
                                     new HBaseMetricsTableModule());
    nonNamespacedFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "core"), 
                                     new CoreDatasetsModule());
    nonNamespacedFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE, "fileSet"), new FileSetModule());
    return nonNamespacedFramework;
  }

  Location renameLocation(String oldLocation, String newLocation) throws URISyntaxException, IOException {
    return renameLocation(new URI(oldLocation), new URI(newLocation));
  }

  Location renameLocation(URI oldLocation, URI newLocation) throws IOException {
    return renameLocation(locationFactory.create(oldLocation), locationFactory.create(newLocation));
  }

  /**
   * Renames the old location to new location if old location exists and the new one does not
   *
   * @param oldLocation the old {@link Location}
   * @param newLocation the new {@link Location}
   * @return new location if and only if the file or directory is successfully moved; null otherwise.
   * @throws IOException
   */
  Location renameLocation(Location oldLocation, Location newLocation) throws IOException {
    if (!newLocation.exists() && oldLocation.exists()) {
      newLocation.mkdirs();
      return oldLocation.renameTo(newLocation);
    }
    return null;
  }

  /**
   * Checks if they given key to be valid from the supplied key prefixes
   *
   * @param key           the key to be validated
   * @param validPrefixes the valid prefixes
   * @return boolean which is true if the key start with validPrefixes else false
   */
  boolean checkKeyValidality(String key, String[] validPrefixes) {
    for (String validPrefix : validPrefixes) {
      if (key.startsWith(validPrefix)) {
        return true;
      }
    }
    return false;
  }
}
