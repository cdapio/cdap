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

package io.cdap.cdap.security.store.secretmanager;

import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.securestore.spi.SecretManager;

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

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // Only allow spi classes.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return resource.startsWith("io/cdap/cdap/securestore/spi");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.startsWith("io.cdap.cdap.securestore.spi");
      }
    };
  }
}
