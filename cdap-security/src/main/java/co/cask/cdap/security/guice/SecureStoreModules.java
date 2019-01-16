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
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.security.store.DefaultSecureStoreService;
import co.cask.cdap.security.store.DummySecureStoreService;
import co.cask.cdap.security.store.FileSecureStoreService;
import co.cask.cdap.security.store.SecureStoreService;
import co.cask.cdap.security.store.SecureStoreUtils;
import co.cask.cdap.security.store.secretmanager.SecretManagerSecureStoreService;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

/**
 * Guice bindings for security store related classes.
 *
 * file - InMemory    - with password       - Valid, returns FileSecureStoreService
 *                    - without password    - Invalid, throws IllegalArgumentException. (Set password)
 *      - Standalone  - with password       - Valid, returns FileSecureStoreService
 *                    - without password    - Invalid, throws IllegalArgumentException. (Set password)
 *      - Distributed - with password       - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - without password    - Invalid, throws IllegalArgumentException. (mode not supported)
 *
 * kms  - InMemory    - > Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *      - Standalone  - > Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (mode not supported)
 *      - Distributed - > Hadoop 2.6        - Valid, returns KMSSecureStoreService
 *                    - < Hadoop 2.6        - Invalid, throws IllegalArgumentException. (Hadoop unsupported)
 *
 * none  - Loads a dummy store. Logs an error message explaining how to setup secure store if any secure store
 *         operations are performed.
 *
 * ext   - Extension based secure store is supported in standalone and distributed mode.
 *
 */
public class SecureStoreModules extends RuntimeModule {
  public static final String DELEGATE_SECURE_STORE_SERVICE = "delegateSecureStoreService";

  @Override
  public final Module getInMemoryModules() {
    return new LocalModule();
  }

  @Override
  public final Module getStandaloneModules() {
    return new LocalModule();
  }

  /**
   * Guice module being used in in memory as well as standalone mode.
   */
  private final class LocalModule extends PrivateModule {

    @Override
    protected void configure() {
      bind(SecureStoreService.class)
        .annotatedWith(Names.named(DELEGATE_SECURE_STORE_SERVICE))
        .toProvider(StoreProvider.class);

      bind(DefaultSecureStoreService.class).in(Scopes.SINGLETON);
      bind(SecureStore.class).to(DefaultSecureStoreService.class);
      bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
      bind(SecureStoreService.class).to(DefaultSecureStoreService.class);
      expose(SecureStore.class);
      expose(SecureStoreManager.class);
      expose(SecureStoreService.class);
    }
  }

  @Override
  public final Module getDistributedModules() {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(SecureStoreService.class)
          .annotatedWith(Names.named(DELEGATE_SECURE_STORE_SERVICE))
          .toProvider(new TypeLiteral<DistributedStoreProvider<SecureStoreService>>() { });

        bind(DefaultSecureStoreService.class).in(Scopes.SINGLETON);
        bind(SecureStore.class).to(DefaultSecureStoreService.class);
        bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
        bind(SecureStoreService.class).to(DefaultSecureStoreService.class);
        expose(SecureStore.class);
        expose(SecureStoreManager.class);
        expose(SecureStoreService.class);
      }
    };
  }

  @Singleton
  private static final class DistributedStoreProvider<T> implements Provider<T> {
    private final CConfiguration cConf;
    private final Injector injector;

    @Inject
    private DistributedStoreProvider(final CConfiguration cConf, Injector injector) {
      this.cConf = cConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get() {
      boolean kmsBacked = SecureStoreUtils.isKMSBacked(cConf);
      if (kmsBacked && SecureStoreUtils.isKMSCapable()) {
        return (T) injector.getInstance(SecureStoreUtils.getKMSSecureStore());
      }
      if (kmsBacked) {
        throw new IllegalArgumentException("Could not find classes required for supporting KMS based secure store. " +
                                             "KMS backed secure store depends on " +
                                             "org.apache.hadoop.crypto.key.kms.KMSClientProvider being available. " +
                                             "This is supported in Apache Hadoop 2.6.0 and up and on " +
                                             "distribution versions that are based on Apache Hadoop 2.6.0 and up.");
      }

      if (SecureStoreUtils.isFileBacked(cConf)) {
        throw new IllegalArgumentException("Only KMS based provider is supported in distributed mode. " +
                   "To be able to use secure store in a distributed environment you" +
                   "will need to use the Hadoop KMS based provider.");
      }

      if (SecureStoreUtils.isExtensionBased(cConf)) {
        return (T) injector.getInstance(SecretManagerSecureStoreService.class);
      }
      return (T) injector.getInstance(DummySecureStoreService.class);
    }
  }

  @Singleton
  private static final class StoreProvider implements Provider<SecureStoreService> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    private StoreProvider(final CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SecureStoreService get() {
      boolean fileBacked = SecureStoreUtils.isFileBacked(cConf);
      boolean validPassword = !Strings.isNullOrEmpty(sConf.get(Constants.Security.Store.FILE_PASSWORD));

      if (fileBacked && validPassword) {
        return injector.getInstance(FileSecureStoreService.class);
      }
      if (fileBacked) {
        throw new IllegalArgumentException("File secure store password is not set. " +
                                             "Please set the \"security.store.file.password\" property in your " +
                                             "cdap-security.xml.");
      }
      if (SecureStoreUtils.isKMSBacked(cConf)) {
        throw new IllegalArgumentException("Only file based secure store is supported in InMemory/Standalone modes. " +
                                             "Please set the \"security.store.provider\" property in cdap-site.xml " +
                                             "to file and set the \"security.store.file.password\" property in " +
                                             "your cdap-security.xml.");
      }

      if (SecureStoreUtils.isExtensionBased(cConf)) {
        return injector.getInstance(SecretManagerSecureStoreService.class);
      }

      return injector.getInstance(DummySecureStoreService.class);
    }
  }
}
