/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.security.spi.credential.CredentialProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Extension loader for {@link CredentialProvider}.
 */
public class CredentialProviderExtensionLoader extends AbstractExtensionLoader<String,
    CredentialProvider> implements CredentialProviderProvider {

  private volatile Set<String> allowedResources;
  private volatile Set<String> allowedPackages;

  @Inject
  CredentialProviderExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.CredentialProvider.EXTENSIONS_DIR));
  }

  @Override
  public Map<String, CredentialProvider> loadCredentialProviders() {
    return getAll();
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(CredentialProvider credentialProvider) {
    return Collections.singleton(credentialProvider.getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only permit cdap-security-spi dependencies
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
        // All cdap-security-spi classes and its dependencies are visible to extensions
        // The set of dependencies for cdap-security-spi should be kept at minimal to reduce
        // dependency conflicts.
        this.allowedResources = resources = ClassPathResources.getResourcesWithDependencies(
            getClass().getClassLoader(), CredentialProvider.class);
        return resources;
      } catch (IOException e) {
        throw new RuntimeException("Failed to find security SPI resources", e);
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

      packages = createPackageSets(getAllowedResources());

      this.allowedPackages = packages;
      return packages;
    }
  }
}