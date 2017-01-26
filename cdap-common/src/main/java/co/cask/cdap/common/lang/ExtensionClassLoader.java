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

package co.cask.cdap.common.lang;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * {@link DirectoryClassLoader} for extensions.
 */
public class ExtensionClassLoader extends DirectoryClassLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionClassLoader.class);

  @VisibleForTesting
  static ClassLoader createParent(ClassLoader baseClassLoader, Class traceFor) {

    final Set<String> extensionResources = traceExtensionDependencies(baseClassLoader, traceFor);
    // by default, FilterClassLoader's defaultFilter allows all hadoop classes, which makes it so that
    // the authorizer extension can share the same instance of UserGroupInformation. This allows kerberos credential
    // renewal to also renew for any extension
    final FilterClassLoader.Filter defaultFilter = FilterClassLoader.defaultFilter();

    return new FilterClassLoader(baseClassLoader, new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return defaultFilter.acceptResource(resource) || extensionResources.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return true;
      }
    });
  }

  private static Set<String> traceExtensionDependencies(ClassLoader baseClassLoader, Class traceFor) {
    try {
      // Trace dependencies for the traceFor class. This will make classes required for extensions available to it.
      return ClassPathResources.getResourcesWithDependencies(baseClassLoader, traceFor);
    } catch (IOException e) {
      LOG.error("Failed to determine resources for class loader while tracing dependencies of " +
                  traceFor, e);
      return ImmutableSet.of();
    }
  }

  public ExtensionClassLoader(File unpackedJarDir, ClassLoader baseClassLoader, Class traceFor) {
    super(unpackedJarDir, createParent(baseClassLoader, traceFor), "lib");
  }
}
