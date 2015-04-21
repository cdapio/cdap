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

package co.cask.cdap.data2.dataset2;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.proto.DatasetSpecificationSummary;
import co.cask.cdap.proto.Id;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * In memory {@link DatasetFramework} implementation that has fixed set of {@link DatasetModule}s.
 * The modules cannot be changed after the creation of the DatasetFramework.
 * Since the modules are fixed, this implementation has better performance than {@link InMemoryDatasetFramework}.
 */
public class StaticDatasetFramework extends InMemoryDatasetFramework implements DatasetFramework {
  private static final String REGISTRY_CACHE_KEY = "registry";
  private static final String MODULES_CACHE_KEY = "modules";

  // Used for caching dataset modules and registry since they do not change after creation.
  private final Cache<String, Object> cache = CacheBuilder.newBuilder().build();

  public StaticDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                Map<String, DatasetModule> modules,
                                CConfiguration configuration) {
    super(registryFactory, modules, configuration);
  }

  @Override
  public Collection<DatasetSpecificationSummary> getInstances(Id.Namespace namespaceId) {
    return super.getInstances(namespaceId);
  }

  @Nullable
  @Override
  public DatasetSpecification getDatasetSpec(Id.DatasetInstance datasetInstanceId) {
    return super.getDatasetSpec(datasetInstanceId);
  }

  @Override
  public boolean hasInstance(Id.DatasetInstance datasetInstanceId) {
    return super.hasInstance(datasetInstanceId);
  }

  @Override
  public boolean hasSystemType(String typeName) {
    return super.hasSystemType(typeName);
  }

  @Override
  public boolean hasType(Id.DatasetType datasetTypeId) {
    return super.hasType(datasetTypeId);
  }

  @Nullable
  @Override
  public <T extends DatasetAdmin> T getAdmin(Id.DatasetInstance datasetInstanceId, @Nullable ClassLoader classLoader)
    throws IOException {
    return super.getAdmin(datasetInstanceId, classLoader);
  }

  @Nullable
  @Override
  public <T extends Dataset> T getDataset(Id.DatasetInstance datasetInstanceId, @Nullable Map<String, String> arguments,
                                          @Nullable ClassLoader classLoader) throws IOException {
    return super.getDataset(datasetInstanceId, arguments, classLoader);
  }

  @Override
  protected DatasetDefinitionRegistry createRegistry(final LinkedHashSet<String> availableModuleClasses,
                                                     @Nullable final ClassLoader classLoader) {
    try {
      // It is okay to have an unchecked cast here, as the same line populates the cache for REGISTRY_CACHE_KEY
      return (DatasetDefinitionRegistry) cache.get(REGISTRY_CACHE_KEY, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return StaticDatasetFramework.super.createRegistry(availableModuleClasses, classLoader);
        }
      });
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  protected LinkedHashSet<String> getAvailableModuleClasses(final Id.Namespace namespace) {
    try {
      // It is okay to have an unchecked cast here, as the same line populates the cache for MODULES_CACHE_KEY
      @SuppressWarnings("unchecked")
      LinkedHashSet<String> modules = (LinkedHashSet<String>) cache.get(MODULES_CACHE_KEY, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          return StaticDatasetFramework.super.getAvailableModuleClasses(namespace);
        }
      });
      return modules;
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void addInstance(String datasetTypeName, Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.addInstance(datasetTypeName, datasetInstanceId, props);
  }

  @Override
  public void updateInstance(Id.DatasetInstance datasetInstanceId, DatasetProperties props)
    throws DatasetManagementException, IOException {
    super.updateInstance(datasetInstanceId, props);
  }

  @Override
  public void deleteInstance(Id.DatasetInstance datasetInstanceId) throws DatasetManagementException, IOException {
    super.deleteInstance(datasetInstanceId);
  }

  @Override
  public void deleteAllInstances(Id.Namespace namespaceId) throws DatasetManagementException, IOException {
    super.deleteAllInstances(namespaceId);
  }

  @Override
  public void addModule(Id.DatasetModule moduleId, DatasetModule module) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void deleteModule(Id.DatasetModule moduleId) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void deleteAllModules(Id.Namespace namespaceId) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void createNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void deleteNamespace(Id.Namespace namespaceId) throws DatasetManagementException {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }
}
