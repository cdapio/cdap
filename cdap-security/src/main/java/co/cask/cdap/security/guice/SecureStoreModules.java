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
import co.cask.cdap.security.store.DummyKMSStore;
import co.cask.cdap.security.store.FileSecureStore;
import co.cask.cdap.security.store.SecureStoreUtils;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
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
        bind(SecureStore.class).toProvider(new TypeLiteral<StoreProvider<SecureStore>>() { });
        bind(SecureStoreManager.class).toProvider(new TypeLiteral<StoreProvider<SecureStoreManager>>() { });
      }
    };
  }

  @Singleton
  private static final class StoreProvider<T> implements Provider<T> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private StoreProvider(final CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
      if (!SecureStoreUtils.isKMSBacked(cConf)) {
        throw new IllegalArgumentException("File based secure store is not supported in distributed mode. " +
                                             "To be able to use secure store in a distributed environment you" +
                                             "will need to use the Hadoop KMS based provider.");
      }
      try {
        if (SecureStoreUtils.isKMSCapable()) {
          return (T) injector.getInstance(Class.forName("co.cask.cdap.security.store.KMSSecureStore"));
        }
      } catch (ClassNotFoundException e) {
        // KMSSecureStore could not be loaded
        LOG.warn("Could not find classes required for supporting KMS based secure store. KMS backed secure store " +
                   "depends on org.apache.hadoop.crypto.key.kms.KMSClientProvider being available. " +
                   "This is supported in Apache Hadoop 2.6.0 and up and on distribution versions that are based " +
                   "on Apache Hadoop 2.6.0 and up.");
      }
      return (T) injector.getInstance(DummyKMSStore.class);
    }
  }
}
