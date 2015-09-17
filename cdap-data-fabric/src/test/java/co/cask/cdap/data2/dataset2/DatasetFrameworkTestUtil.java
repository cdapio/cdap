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
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.proto.Id;
import co.cask.tephra.DefaultTransactionExecutor;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionExecutor;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.inmemory.InMemoryTxSystemClient;
import co.cask.tephra.inmemory.MinimalTxSystemClient;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public final class DatasetFrameworkTestUtil extends ExternalResource {
  public static final Id.Namespace NAMESPACE_ID = Id.Namespace.from("myspace");

  private Injector injector;
  private CConfiguration cConf;
  private TemporaryFolder tmpFolder;
  private DatasetFramework framework;
  private TransactionManager txManager;

  public Injector getInjector() {
    return injector;
  }

  public CConfiguration getConfiguration() {
    return cConf;
  }

  @Override
  protected void before() throws Throwable {
    this.tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    File localDataDir = tmpFolder.newFolder();
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new PrivateModule() {
        @Override
        protected void configure() {
          install(new FactoryModuleBuilder()
                    .implement(DatasetDefinitionRegistry.class, DefaultDatasetDefinitionRegistry.class)
                    .build(DatasetDefinitionRegistryFactory.class));
          bind(DatasetFramework.class).to(InMemoryDatasetFramework.class);
          expose(DatasetFramework.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    framework = injector.getInstance(DatasetFramework.class);
  }

  @Override
  protected void after() {
    if (txManager != null) {
      txManager.stopAndWait();
    }
    if (tmpFolder != null) {
      tmpFolder.delete();
    }
  }

  public DatasetFramework getFramework() {
    return framework;
  }

  public void addModule(Id.DatasetModule moduleId, DatasetModule module) throws DatasetManagementException {
    framework.addModule(moduleId, module);
  }

  public void deleteModule(Id.DatasetModule moduleId) throws DatasetManagementException {
    framework.deleteModule(moduleId);
  }

  public void createInstance(String type, Id.DatasetInstance datasetInstanceId, DatasetProperties properties)
    throws IOException, DatasetManagementException {
    framework.addInstance(type, datasetInstanceId, properties);
  }

  public void deleteInstance(Id.DatasetInstance datasetInstanceId)
    throws IOException, DatasetManagementException {
    framework.deleteInstance(datasetInstanceId);
  }

  public <T extends Dataset> T getInstance(Id.DatasetInstance datasetInstanceId)
    throws DatasetManagementException, IOException {
    return getInstance(datasetInstanceId, DatasetDefinition.NO_ARGUMENTS);
  }

  public <T extends Dataset> T getInstance(Id.DatasetInstance datasetInstanceId, Map<String, String> arguments)
    throws DatasetManagementException, IOException {
    return framework.getDataset(datasetInstanceId, arguments, null);
  }

  public DatasetSpecification getSpec(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException {
    return framework.getDatasetSpec(datasetInstanceId);
  }

  public TransactionExecutor newTransactionExecutor(TransactionAware...tables) {
    Preconditions.checkArgument(tables != null);
    return new DefaultTransactionExecutor(new MinimalTxSystemClient(), tables);
  }

  public TransactionExecutor newInMemoryTransactionExecutor(TransactionAware...tables) {
    Preconditions.checkArgument(tables != null);
    return new DefaultTransactionExecutor(new InMemoryTxSystemClient(txManager), tables);
  }

  public TransactionManager getTxManager() {
    return txManager;
  }
}
