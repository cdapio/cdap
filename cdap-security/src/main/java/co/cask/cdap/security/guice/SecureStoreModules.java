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
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.common.security.SecureStoreUtils;
import co.cask.cdap.security.store.DummyKMSStore;
import co.cask.cdap.security.store.FileSecureStore;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice bindings for security store related classes.
 */
public class SecureStoreModules extends RuntimeModule {

  private static final Logger LOG = LoggerFactory.getLogger(SecureStoreModules.class);
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
      if (SecureStoreUtils.isKMSBacked(cConf)) {
        try {
          // Check if required KMS classes are present.
          Class.forName("org.apache.hadoop.crypto.key.kms.KMSClientProvider");
          LOG.warn("Could not find classes required for supporting KMS provider.");
          // No Exception was thrown, that means the classes are available, load our provider
          return (SecureStore) injector.getInstance(Class.forName("co.cask.cdap.security.store.KMSSecureStore"));
        } catch (ClassNotFoundException e) {
          // Required KMS classes are not present.
          return injector.getInstance(DummyKMSStore.class);
        }
      } else {
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
      if (SecureStoreUtils.isKMSBacked(cConf)) {
        try {
          // Check if required KMS classes are present.
          Class.forName("org.apache.hadoop.crypto.key.kms.KMSClientProvider");
          LOG.warn("Could not find classes required for supporting KMS provider.");
          // No Exception was thrown, that means the classes are available, load our provider
          return (SecureStoreManager) injector.getInstance(Class.forName("co.cask.cdap.security.store.KMSSecureStore"));
        } catch (ClassNotFoundException e) {
          // Required KMS classes are not present.
          return injector.getInstance(DummyKMSStore.class);
        }
      } else {
        return injector.getInstance(FileSecureStore.class);
      }
    }
  }
}
