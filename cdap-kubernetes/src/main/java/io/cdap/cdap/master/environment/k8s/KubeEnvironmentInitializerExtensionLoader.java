/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.k8s.spi.environment.KubeEnvironmentInitializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Loads Kubernetes environment initializers from the extensions directory.
 */
public class KubeEnvironmentInitializerExtensionLoader
  extends AbstractExtensionLoader<String, KubeEnvironmentInitializer> implements KubeEnvironmentInitializerProvider {

  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(KubeEnvironmentInitializer.class.getClassLoader(),
                                                             KubeEnvironmentInitializer.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for k8s environment initializer extension. " +
                                   "Kubernetes environment initialization may fail.", e);
    }
  }

  KubeEnvironmentInitializerExtensionLoader(String extensionDir) {
    super(extensionDir);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(KubeEnvironmentInitializer kubeEnvironmentInitializer) {
    return Collections.singleton(kubeEnvironmentInitializer.getSpecification().getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // filter all non-spi classes to provide isolation from CDAP's classes.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return ALLOWED_RESOURCES.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return ALLOWED_PACKAGES.contains(packageName);
      }
    };
  }

  @Override
  public Map<String, KubeEnvironmentInitializer> loadKubeEnvironmentInitializers() {
    return getAll();
  }
}
