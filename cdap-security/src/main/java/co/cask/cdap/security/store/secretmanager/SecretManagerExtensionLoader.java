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

package co.cask.cdap.security.store.secretmanager;

import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.extension.AbstractExtensionLoader;
import co.cask.cdap.securestore.spi.SecretManager;

import java.util.Collections;
import java.util.Set;

/**
 * Secret Manager extension loader which loads secret manager implementation.
 */
class SecretManagerExtensionLoader extends AbstractExtensionLoader<String, SecretManager> {

  SecretManagerExtensionLoader(String extensionDir) {
    super(extensionDir);
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(SecretManager secretManager) {
    return Collections.singleton(secretManager.getName());
  }

  // filter all non-spi classes to provide isolation from CDAP's classes.
  @Override
  protected ClassLoader getExtensionParentClassLoader() {
    return new FilterClassLoader(super.getExtensionParentClassLoader(), new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        // we need to allow spi classes and slf4j classes from parent classloader. This is because secret manager spi
        // depends on slf4j and implementation will have this dependency in provided scope.
        return resource.startsWith("co/cask/cdap/securestore/spi") || resource.startsWith("org/slf4j");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        // we need to allow spi classes and slf4j classes from parent classloader. This is because secret manager spi
        // depends on slf4j and implementation will have this dependency in provided scope.
        return packageName.startsWith("co.cask.cdap.securestore.spi") || packageName.startsWith("org.slf4j");
      }
    });
  }
}
