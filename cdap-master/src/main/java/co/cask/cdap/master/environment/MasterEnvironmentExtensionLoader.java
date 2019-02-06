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

package co.cask.cdap.master.environment;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.extension.AbstractExtensionLoader;
import co.cask.cdap.master.spi.environment.MasterEnvironment;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An extension loader for {@link MasterEnvironment}.
 */
public class MasterEnvironmentExtensionLoader extends AbstractExtensionLoader<String, MasterEnvironment> {

  private volatile Set<String> allowedResources;
  private volatile Set<String> allowedPackages;

  public MasterEnvironmentExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Master.EXTENSIONS_DIR));
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(MasterEnvironment masterEnvironment) {
    return Collections.singleton(masterEnvironment.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only permit cdap-master-spi dependencies
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return getAllowedResources().contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return getAllowedPackages().contains(packageName);
      }
    };
  }

  /**
   * Returns the set of resources that are visible to extensions.
   */
  private Set<String> getAllowedResources() {
    Set<String> resources = this.allowedResources;
    if (resources != null) {
      return resources;
    }

    synchronized (this) {
      resources = this.allowedResources;
      if (resources != null) {
        return resources;
      }
      try {
        // All cdap-master-spi classes and its dependencies are visible to extensions
        // The set of dependencies for cdap-master-spi should be kept at minimal to reduce dependency conflicts
        this.allowedResources = resources = ClassPathResources.getResourcesWithDependencies(getClass().getClassLoader(),
                                                                                            MasterEnvironment.class);
        return resources;
      } catch (IOException e) {
        throw new RuntimeException("Failed to find master SPI resources", e);
      }
    }
  }

  /**
   * Returns the set of package names that are visible to extensions.
   */
  private Set<String> getAllowedPackages() {
    Set<String> packages = this.allowedPackages;
    if (packages != null) {
      return packages;
    }
    synchronized (this) {
      packages = this.allowedPackages;
      if (packages != null) {
        return packages;
      }

      packages = new HashSet<>();
      Set<String> allowResources = getAllowedResources();
      for (String resource : allowResources) {
        if (resource.endsWith(".class")) {
          int idx = resource.lastIndexOf("/");
          if (idx >= 0) {
            packages.add(resource.substring(0, idx));
          }
        }
      }

      this.allowedPackages = packages;
      return packages;
    }
  }
}
