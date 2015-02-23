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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.config.DefaultConfigStore;
import co.cask.cdap.data2.datafabric.dataset.DatasetMetaTableUtil;
import co.cask.cdap.internal.app.runtime.schedule.store.ScheduleStoreTableUtil;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.metrics.store.DefaultMetricDatasetFactory;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.tephra.TransactionExecutorFactory;
import co.cask.tephra.TransactionSystemClient;
import co.cask.tephra.distributed.TransactionService;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * Does the necessary initial setup before Upgrading difference Services and Modules
 */
public class UpgradeSetup extends AbstractUpgrade implements Upgrade {

  @Override
  public void upgrade(Injector injector) throws Exception {

    // Setting up all system datasets to be upgraded, collecting them from respective components
    namespacedFramework = createRegisteredDatasetFramework(injector);
    nonNamespaedFramework = createNonNamespaceDSFramework(injector);
    // dataset service
    DatasetMetaTableUtil.setupDatasets(namespacedFramework);
    // app metadata
    DefaultStore.setupDatasets(namespacedFramework);
    // config store
    DefaultConfigStore.setupDatasets(namespacedFramework);
    // logs metadata
    LogSaverTableUtil.setupDatasets(namespacedFramework);
    // scheduler metadata
    ScheduleStoreTableUtil.setupDatasets(namespacedFramework);

    initialize(injector);
    startServices();
    instantiateDefaultStore(injector);
    createDefaultNamespace();

    // metrics data
    DefaultMetricDatasetFactory.setupDatasets(cConf, namespacedFramework);
  }

  private void initialize(Injector injector) {
    cConf = injector.getInstance(CConfiguration.class);
    txService = injector.getInstance(TransactionService.class);
    zkClientService = injector.getInstance(ZKClientService.class);
    locationFactory = injector.getInstance(LocationFactory.class);
    txClient = injector.getInstance(TransactionSystemClient.class);
    executorFactory = injector.getInstance(TransactionExecutorFactory.class);
  }

  /**
   * Starts the required services to use datasets etc.
   */
  private void startServices() {
    zkClientService.startAndWait();
    txService.startAndWait();
  }

  /**
   * Instantiate {@link DefaultStore}
   *
   * @param injector the {@link Injector}
   */
  private void instantiateDefaultStore(Injector injector) {
    defaultStore = new DefaultStore(injector.getInstance(CConfiguration.class), locationFactory,
                                    injector.getInstance(TransactionSystemClient.class),
                                    nonNamespaedFramework);
  }

  /**
   * Creates the {@link Constants#DEFAULT_NAMESPACE} namespace
   */
  private void createDefaultNamespace() {
    defaultStore.createNamespace(new NamespaceMeta.Builder().setId(Constants.DEFAULT_NAMESPACE)
                                   .setName(Constants.DEFAULT_NAMESPACE)
                                   .setDescription(Constants.DEFAULT_NAMESPACE)
                                   .build());
  }
}
