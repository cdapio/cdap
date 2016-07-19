/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.dataset.module.DatasetDefinitionRegistry;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import com.google.common.base.Objects;

import javax.annotation.Nullable;

/**
 * Utility class to provide command methods for interacting with {@link DatasetDefinitionRegistry}.
 */
public final class DatasetDefinitionRegistries {

  public static void register(String moduleClassName,
                              @Nullable ClassLoader classLoader, DatasetDefinitionRegistry registry)
    throws ClassNotFoundException, IllegalAccessException, InstantiationException, TypeConflictException {

    ClassLoader systemClassLoader = DatasetDefinitionRegistries.class.getClassLoader();

    // Either uses the given classloader or the system one
    ClassLoader moduleClassLoader = Objects.firstNonNull(classLoader, systemClassLoader);

    Class<? extends DatasetModule> moduleClass;
    try {
      moduleClass = loadClass(moduleClassLoader, moduleClassName);
    } catch (ClassNotFoundException e) {
      // If failed to load from the given classloader (if not null), try to load it from system classloader
      if (classLoader == null || classLoader.equals(systemClassLoader)) {
        throw e;
      }
      try {
        moduleClass = loadClass(systemClassLoader, moduleClassName);
      } catch (ClassNotFoundException e2) {
        e.addSuppressed(e2);
        throw e;
      }
    }

    DatasetModule module = DatasetModules.getDatasetModule(moduleClass);
    module.register(registry);
  }


  /**
   * Loads a {@link Class} from the given {@link ClassLoader} with the context ClassLoader
   * set to the given ClassLoader and reset it after loading is done.
   */
  @SuppressWarnings("unchecked")
  private static <T> Class<T> loadClass(ClassLoader classLoader, String className) throws ClassNotFoundException {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(classLoader);
    try {
      return (Class<T>) classLoader.loadClass(className);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }


  private DatasetDefinitionRegistries() {
    // private for util class
  }
}
