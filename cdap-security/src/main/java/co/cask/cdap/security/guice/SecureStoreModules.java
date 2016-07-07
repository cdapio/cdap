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

package co.cask.cdap.security.guice;

import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.security.store.FileSecureStore;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;

/**
 *
 */
public class SecureStoreModules extends RuntimeModule {
  /**
   * Implementers of this method should return a combined Module that includes
   * all of the modules and classes required to instantiate and run an
   * in-memory CDAP instance.
   *
   * @return A combined set of Modules required for InMemory execution.
   */
  @Override
  public final Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).to(FileSecureStore.class);
        bind(SecureStoreManager.class).to(FileSecureStore.class);
      }
    };
  }

  @Override
  public final Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).to(FileSecureStore.class);
        bind(SecureStoreManager.class).to(FileSecureStore.class);
      }
    };
  }

  @Override
  public final Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(SecureStore.class).toProvider(SecureStoreProvider.class);
        bind(SecureStoreManager.class).toProvider(SecureStoreManagerProvider.class);
      }
    };
  }

  private static class SecureStoreProvider implements Provider<SecureStore> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private SecureStoreProvider(final CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    /**
     * Provides an instance of {@link SecureStore}. Must never return {@code null}.
     */
    @Override
    public SecureStore get() {
      if ("file".equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER))) {
        return injector.getInstance(FileSecureStore.class);
      } else {
        // TODO: Change this to use KMS once that is implemented.
        return injector.getInstance(FileSecureStore.class);
      }
    }
  }

  private static class SecureStoreManagerProvider implements Provider<SecureStoreManager> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private SecureStoreManagerProvider(final CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    /**
     * Provides an instance of {@link SecureStoreManager}. Must never return {@code null}.
     */
    @Override
    public SecureStoreManager get() {
      if ("file".equalsIgnoreCase(cConf.get(Constants.Security.Store.PROVIDER))) {
        return injector.getInstance(FileSecureStore.class);
      } else {
        // TODO: Change this to use KMS once that is implemented.
        return injector.getInstance(FileSecureStore.class);
      }
    }
  }
}
