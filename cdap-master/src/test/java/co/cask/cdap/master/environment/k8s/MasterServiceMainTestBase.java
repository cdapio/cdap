/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.app.guice.ConstantTransactionSystemClient;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableAdmin;
import co.cask.cdap.data2.nosql.NoSqlStructuredTableRegistry;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import co.cask.cdap.spi.data.table.StructuredTableRegistry;
import co.cask.cdap.store.StoreDefinition;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Writer;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * A unit-test that starts all master service main classes.
 */
public class MasterServiceMainTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static Map<Class<?>, ServiceMainManager<?>> serviceManagers = new HashMap<>();

  @BeforeClass
  public static void init() throws Exception {
    zkServer = InMemoryZKServer.builder().setAutoCleanDataDir(false).setDataDir(TEMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    CConfiguration cConf = CConfiguration.create();

    // Set the HDFS directory as well as we are using DFSLocationModule in the master services
    cConf.set(Constants.CFG_HDFS_NAMESPACE, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.Zookeeper.QUORUM, zkServer.getConnectionStr());

    // Set all bind address to localhost
    String localhost = InetAddress.getLoopbackAddress().getHostName();
    StreamSupport.stream(CConfiguration.create().spliterator(), false)
      .map(Map.Entry::getKey)
      .filter(s -> s.endsWith(".bind.address"))
      .forEach(key -> cConf.set(key, localhost));

    // Start the master main services
    serviceManagers.put(MessagingServiceMain.class, runMain(MessagingServiceMain.class, cConf));
    serviceManagers.put(MetricsServiceMain.class, runMain(MetricsServiceMain.class, cConf));
    serviceManagers.put(AppFabricServiceMain.class, runMain(AppFabricServiceMain.class, cConf));
  }

  @AfterClass
  public static void finish() {
    // Reverse stop services
    Lists.reverse(new ArrayList<>(serviceManagers.values())).forEach(ServiceMainManager::cancel);
    zkServer.stopAndWait();
  }

  /**
   * Gets the instance of master main service of the given class.
   */
  static <T extends AbstractServiceMain> T getServiceMainInstance(Class<T> serviceMainClass) {
    ServiceMainManager<?> manager = serviceManagers.get(serviceMainClass);
    AbstractServiceMain instance = manager.getInstance();
    if (!serviceMainClass.isInstance(instance)) {
      throw new IllegalArgumentException("Mismatch manager class." + serviceMainClass + " != " + instance);
    }
    //noinspection unchecked
    return (T) instance;
  }

  /**
   * Instantiate and start the given service main class.
   *
   * @param serviceMainClass the service main class to start
   * @param originalCConf the base {@link CConfiguration} for the service
   * @param <T> type of the service main class
   * @return A {@link ServiceMainManager} to interface with the service instance
   * @throws Exception if failed to start the service
   */
  private static <T extends AbstractServiceMain> ServiceMainManager<T> runMain(Class<T> serviceMainClass,
                                                                               CConfiguration originalCConf)
    throws Exception {

    // Set a unique local data directory for each service
    CConfiguration cConf = CConfiguration.copy(originalCConf);
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    // Create StructuredTable stores before starting the main. The registry will be preserved and pick by the main class
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getStandaloneModules()
    );
    DatasetDefinition tableDef = injector.getInstance(Key.get(DatasetDefinition.class,
                                                              Names.named(Constants.Dataset.TABLE_TYPE)));
    StructuredTableRegistry tableRegistry = new NoSqlStructuredTableRegistry(tableDef,
                                                                             new ConstantTransactionSystemClient());
    StructuredTableAdmin tableAdmin = new NoSqlStructuredTableAdmin(tableDef, tableRegistry);
    StoreDefinition.createAllTables(tableAdmin, tableRegistry);
    injector.getInstance(LevelDBTableService.class).close();

    // Write the "cdap-site.xml" and pass the directory to the main service
    File confDir = TEMP_FOLDER.newFolder();
    try (Writer writer = Files.newBufferedWriter(new File(confDir, "cdap-site.xml").toPath())) {
      cConf.writeXml(writer);
    }

    T service = serviceMainClass.newInstance();
    service.init(new String[] { "--env=mock", "--conf=" + confDir.getAbsolutePath() });
    service.start();

    return new ServiceMainManager<T>() {
      @Override
      public T getInstance() {
        return service;
      }

      @Override
      public void cancel() {
        service.stop();
        service.destroy();
      }
    };
  }

  /**
   * Represents a started main service.
   * @param <T> type of the service main class
   */
  private interface ServiceMainManager<T extends AbstractServiceMain> extends Cancellable {
    T getInstance();
  }
}
