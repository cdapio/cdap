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
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.lib.file.FileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.PartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.partitioned.TimePartitionedFileSetModule;
import co.cask.cdap.data2.dataset2.lib.table.CoreDatasetsModule;
import co.cask.cdap.data2.dataset2.lib.table.ObjectMappedTableModule;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.proto.Id;
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

  protected static final Id.Namespace NAMESPACE_ID = Id.Namespace.from("myspace");
  protected static final DatasetNamespace DS_NAMESPACE = new DefaultDatasetNamespace(CConfiguration.create());

  protected static DatasetFramework framework;
  private static final Id.DatasetModule inMemory = Id.DatasetModule.from(NAMESPACE_ID, "inMemory");
  private static final Id.DatasetModule core = Id.DatasetModule.from(NAMESPACE_ID, "core");
  private static final Id.DatasetModule fileSet = Id.DatasetModule.from(NAMESPACE_ID, "fileSet");
  private static final Id.DatasetModule tpfs = Id.DatasetModule.from(NAMESPACE_ID, "tpfs");
  private static final Id.DatasetModule pfs = Id.DatasetModule.from(NAMESPACE_ID, "pfs");
  private static final Id.DatasetModule omt = Id.DatasetModule.from(NAMESPACE_ID, "objectMappedTable");

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
    framework.addModule(inMemory, new InMemoryTableModule());
    framework.addModule(core, new CoreDatasetsModule());
    framework.addModule(fileSet, new FileSetModule());
    framework.addModule(tpfs, new TimePartitionedFileSetModule());
    framework.addModule(pfs, new PartitionedFileSetModule());
    framework.addModule(omt, new ObjectMappedTableModule());
  }

  @AfterClass
  public static void destroy() throws Exception {
    framework.deleteModule(omt);
    framework.deleteModule(pfs);
    framework.deleteModule(tpfs);
    framework.deleteModule(fileSet);
    framework.deleteModule(core);
    framework.deleteModule(inMemory);
  }

  protected static void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {
    framework.addModule(moduleId, module);
  }

  protected static void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    framework.deleteModule(moduleId);
  }

  protected static void createInstance(String type, Id.DatasetInstance datasetInstanceId, DatasetProperties properties)
    throws IOException, DatasetManagementException {

    framework.addInstance(type, datasetInstanceId, properties);
  }

  protected static void deleteInstance(Id.DatasetInstance datasetInstanceId)
    throws IOException, DatasetManagementException {
    framework.deleteInstance(datasetInstanceId);
  }

  protected static <T extends Dataset> T getInstance(Id.DatasetInstance datasetInstanceId)
    throws DatasetManagementException, IOException {
    return getInstance(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS);
  }

  protected static <T extends Dataset> T getInstance(Id.DatasetInstance datasetInstanceId,
                                                     Map<String, String> arguments)
    throws DatasetManagementException, IOException {
    return framework.getDataset(datasetInstanceId, arguments, null);
  }

  protected DatasetSpecification getSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return framework.getDatasetSpec(datasetInstanceId);
  }

  protected static TransactionExecutor newTransactionExecutor(TransactionAware...tables) {
    Preconditions.checkArgument(tables != null);
    return new DefaultTransactionExecutor(new MinimalTxSystemClient(), tables);
  }
}
