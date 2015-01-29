/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryOrderedTableModule;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.inmemory.MinimalTxSystemClient;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class AbstractDatasetTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static DatasetFramework framework;

  @BeforeClass
  public static void init() throws Exception {

    File localDataDir = tmpFolder.newFolder();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());

    final Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules());

    framework = new InMemoryDatasetFramework(new DatasetDefinitionRegistryFactory() {
      @Override
      public DatasetDefinitionRegistry create() {
        DefaultDatasetDefinitionRegistry registry = new DefaultDatasetDefinitionRegistry();
        injector.injectMembers(registry);
        return registry;
      }
    }, cConf);
    framework.addModule("inMemory", new InMemoryOrderedTableModule());
    framework.addModule("core", new CoreDatasetsModule());
    framework.addModule("fileSet", new FileSetModule());
  }

  @AfterClass
  public static void destroy() throws Exception {
    framework.deleteModule("core");
    framework.deleteModule("inMemory");
  }

  protected static void addModule(String name, DatasetModule module) throws DatasetManagementException {
    framework.addModule(name, module);
  }

  protected static void deleteModule(String name) throws DatasetManagementException {
    framework.deleteModule(name);
  }

  protected static void createInstance(String type, String instanceName, DatasetProperties properties)
    throws IOException, DatasetManagementException {

    framework.addInstance(type, instanceName, properties);
  }

  protected static void deleteInstance(String instanceName) throws IOException, DatasetManagementException {
    framework.deleteInstance(instanceName);
  }

  protected static <T extends Dataset> T getInstance(String datasetName)
    throws DatasetManagementException, IOException {

    return getInstance(datasetName, null);
  }

  protected static <T extends Dataset> T getInstance(String datasetName, Map<String, String> arguments)
    throws DatasetManagementException, IOException {
    return framework.getDataset(datasetName, arguments, null);
  }

  protected static TransactionExecutor newTransactionExecutor(TransactionAware...tables) {
    Preconditions.checkArgument(tables != null);
    return new DefaultTransactionExecutor(new MinimalTxSystemClient(), tables);
  }
}
