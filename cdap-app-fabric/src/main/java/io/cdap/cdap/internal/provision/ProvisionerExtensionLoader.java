/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Loads provisioners from the extensions directory.
 */
public class ProvisionerExtensionLoader extends AbstractExtensionLoader<String, Provisioner>
  implements ProvisionerProvider {

  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(Provisioner.class.getClassLoader(), Provisioner.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for provisioner extension. " +
                                   "Usage of provisioner might fail.", e);
    }
  }

  private static Set<String> createPackageSets(Set<String> resources) {
    return resources.stream()
      .map(resource -> {
        int idx = resource.lastIndexOf("/");
        return idx < 0 ? "" : resource.substring(0, idx).replace('/', '.');
      })
      .filter(s -> !s.isEmpty())
      .collect(Collectors.toSet());
  }

  @Inject
  ProvisionerExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Provisioner.EXTENSIONS_DIR));
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(Provisioner provisioner) {
    return Collections.singleton(provisioner.getSpec().getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // filter all non-spi classes to provide isolation from CDAP's classes. For example, dataproc provisioner uses
    // a different guava than CDAP's guava.
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
  public Map<String, Provisioner> loadProvisioners() {
    Map<String, Provisioner> provisioners = new HashMap<>();
    // always include the native provisioner
    Provisioner nativeProvisioner = new NativeProvisioner();
    provisioners.put(nativeProvisioner.getSpec().getName(), nativeProvisioner);
    provisioners.putAll(getAll());
    return provisioners;
  }
}
