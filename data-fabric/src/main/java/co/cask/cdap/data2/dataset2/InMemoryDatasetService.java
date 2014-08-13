/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.explore.client.DatasetExploreFacade;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * In-memory implementation of the {@link DatasetFramework} to be used by app server
 */
@Singleton
public class InMemoryDatasetService implements DatasetFramework {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatasetService.class);

  private static final InMemoryDatasetService INSTANCE = new InMemoryDatasetService();

  private InMemoryDatasetFramework delegate = null;
  private DatasetExploreFacade exploreFacade;

  public static InMemoryDatasetService getInstance() {
    return INSTANCE;
  }

  @Inject
  public synchronized void set(DatasetDefinitionRegistryFactory registryFactory,
                               @Named("defaultDatasetModules") Map<String, ? extends DatasetModule> defaultModules) {
    if (delegate != null) {
      return;
    }

    delegate = new InMemoryDatasetFramework(registryFactory, defaultModules);
  }

  @Inject(optional = true)
  public synchronized void set(DatasetExploreFacade datasetExploreFacade) {
    exploreFacade = datasetExploreFacade;
  }

  @Override
  public void addModule(String moduleName, DatasetModule module) throws DatasetManagementException {
    delegate.addModule(moduleName, module);
  }

  @Override
  public void deleteModule(String moduleName) throws DatasetManagementException {
    delegate.deleteModule(moduleName);
  }

  @Override
  public void deleteAllModules() throws DatasetManagementException {
    delegate.deleteAllModules();
  }

  @Override
  public void addInstance(String datasetTypeName, String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, IOException {

    delegate.addInstance(datasetTypeName, datasetInstanceName, props);

    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - REACTOR-314
    try {
      exploreFacade.enableExplore(datasetInstanceName);
      LOG.info("Enabled explore for dataset: " + datasetInstanceName);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s with props %s: %s",
                                 datasetInstanceName, datasetTypeName, props, e.getMessage());
      LOG.warn(msg);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  @Override
  public void updateInstance(String datasetInstanceName, DatasetProperties props)
    throws DatasetManagementException, IOException {

    delegate.updateInstance(datasetInstanceName, props);

    // Enable ad-hoc exploration of dataset
    // Note: today explore enable is not transactional with dataset create - REACTOR-314
    try {
      exploreFacade.disableExplore(datasetInstanceName);
      exploreFacade.enableExplore(datasetInstanceName);
    } catch (Exception e) {
      String msg = String.format("Cannot enable exploration of dataset instance %s of type %s: %s",
                                 datasetInstanceName, props, e.getMessage());
      LOG.warn(msg);
      // TODO: at this time we want to still allow using dataset even if it cannot be used for exploration
      //responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, msg);
      //return;
    }
  }

  @Override
  public Collection<DatasetSpecification> getInstances() throws DatasetManagementException {
    return delegate.getInstances();
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(String name) throws DatasetManagementException {
    return delegate.getDatasetSpec(name);
  }

  @Override
  public boolean hasInstance(String instanceName) throws DatasetManagementException {
    return delegate.hasInstance(instanceName);
  }

  @Override
  public boolean hasType(String typeName) throws DatasetManagementException {
    return delegate.hasType(typeName);
  }

  @Override
  public void deleteInstance(String datasetInstanceName) throws DatasetManagementException, IOException {
    // First disable ad-hoc exploration of dataset
    // Note: today explore disable is not transactional with dataset delete - REACTOR-314
    try {
      exploreFacade.disableExplore(datasetInstanceName);
    } catch (Exception e) {
      String msg = String.format("Cannot disable exploration of dataset instance %s: %s",
                                 datasetInstanceName, e.getMessage());
      LOG.warn(msg);
      // TODO: at this time we want to still drop dataset even if it cannot be disabled for exploration
//      throw e;
    }

    delegate.deleteInstance(datasetInstanceName);
  }

  @Override
  public void deleteAllInstances() throws DatasetManagementException, IOException {
    delegate.deleteAllInstances();
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(String datasetInstanceName, @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getAdmin(datasetInstanceName, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(String datasetInstanceName,
                                          @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader)
    throws DatasetManagementException, IOException {

    return delegate.getDataset(datasetInstanceName, arguments, classLoader);
  }
}
