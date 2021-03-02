/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.security.guice;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.security.auth.InMemoryKeyManager;
import io.cdap.cdap.security.auth.KeyManager;

/**
 * Guice bindings for InMemoryKeyManagers. This extends {@code SecurityModule} to provide
 * an instance of {@code InMemoryKeyManager}.
 */
final class InMemorySecurityModule extends SecurityModule {

  @Override
  protected void bindKeyManager(Binder binder) {
    binder.bind(KeyManager.class).toProvider(InMemoryKeyManagerProvider.class).in(Scopes.SINGLETON);
  }

  private static final class InMemoryKeyManagerProvider implements Provider<KeyManager> {
    private final CConfiguration cConf;

    @Inject
    InMemoryKeyManagerProvider(CConfiguration conf) {
      this.cConf = conf;
    }

    @Override
    public KeyManager get() {
      return new InMemoryKeyManager(cConf);
    }
  }
}
