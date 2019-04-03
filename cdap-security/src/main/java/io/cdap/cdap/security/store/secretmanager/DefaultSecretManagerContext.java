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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.securestore.spi.SecretManagerContext;
import co.cask.cdap.securestore.spi.SecretStore;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link SecretManagerContext}.
 */
public class DefaultSecretManagerContext implements SecretManagerContext {
  private final CConfiguration cConf;
  private final SecretStore store;

  DefaultSecretManagerContext(CConfiguration cConf, SecretStore store) {
    this.cConf = cConf;
    this.store = store;
  }

  @Override
  public Map<String, String> getProperties() {
    String prefix = String.format("%s%s.", Constants.Security.Store.PROPERTY_PREFIX,
                                  cConf.get(Constants.Security.Store.PROVIDER));
    return Collections.unmodifiableMap(cConf.getPropsWithPrefix(prefix));
  }

  @Override
  public SecretStore getSecretStore() {
    return store;
  }
}
