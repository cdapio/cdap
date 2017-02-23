/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.proto.id.DatasetModuleId;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.twill.filesystem.Location;

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
public class StaticDatasetFramework extends InMemoryDatasetFramework {
  private static final String REGISTRY_CACHE_KEY = "registry";
  private static final String MODULES_CACHE_KEY = "modules";

  // Used for caching dataset modules and registry since they do not change after creation.
  private final Cache<String, Object> cache = CacheBuilder.newBuilder().build();

  public StaticDatasetFramework(DatasetDefinitionRegistryFactory registryFactory,
                                Map<String, DatasetModule> modules) {
    super(registryFactory, modules);
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
  protected LinkedHashSet<String> getAvailableModuleClasses(final NamespaceId namespace) {
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
  public void addModule(DatasetModuleId moduleId, DatasetModule module) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void addModule(DatasetModuleId moduleId, DatasetModule module,
                        Location jarLocation) throws DatasetManagementException {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void deleteModule(DatasetModuleId moduleId) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }

  @Override
  public void deleteAllModules(NamespaceId namespaceId) {
    throw new UnsupportedOperationException("Cannot change modules of "
                                              + StaticDatasetFramework.class.getSimpleName());
  }
}
