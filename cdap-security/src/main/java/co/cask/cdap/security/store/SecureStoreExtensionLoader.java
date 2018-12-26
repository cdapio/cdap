/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.cdap.security.store;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.extension.AbstractExtensionLoader;
import co.cask.cdap.securestore.spi.SecretsManager;

import java.util.Collections;
import java.util.Set;

/**
 * Secure Store extension loader.
 */
public class SecureStoreExtensionLoader extends AbstractExtensionLoader<String, SecretsManager> {

  public SecureStoreExtensionLoader(CConfiguration cConf) {
    super(cConf.get(Constants.Security.Store.EXTENSIONS_DIR));
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(SecretsManager secretsManager) {
    return Collections.singleton(secretsManager.getSecretsManagerName());
  }

  // filter all non-spi classes to provide isolation from CDAP's classes.
  @Override
  protected ClassLoader getExtensionParentClassLoader() {
    return new FilterClassLoader(super.getExtensionParentClassLoader(), new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return resource.startsWith("co/cask/cdap/securestore/spi");
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return packageName.startsWith("co/cask/cdap/securestore/spi");
      }
    });
  }
}
