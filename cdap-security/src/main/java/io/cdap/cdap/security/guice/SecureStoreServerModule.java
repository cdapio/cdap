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

package io.cdap.cdap.security.guice;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Names;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.security.store.DefaultSecureStoreService;
import io.cdap.cdap.security.store.DummySecureStoreService;
import io.cdap.cdap.security.store.FileSecureStoreService;
import io.cdap.cdap.security.store.SecureStoreService;
import io.cdap.cdap.security.store.SecureStoreUtils;
import io.cdap.cdap.security.store.secretmanager.SecretManagerSecureStoreService;

/**
 * Server side guice bindings for secure store service related classes.
 */
public class SecureStoreServerModule extends PrivateModule {
  public static final String DELEGATE_SECURE_STORE_SERVICE = "delegateSecureStoreService";

  @Override
  protected void configure() {
    bind(SecureStoreService.class)
      .annotatedWith(Names.named(DELEGATE_SECURE_STORE_SERVICE))
      .toProvider(SecureStoreServiceProvider.class);

    bind(DefaultSecureStoreService.class).in(Scopes.SINGLETON);
    bind(SecureStore.class).to(DefaultSecureStoreService.class);
    bind(SecureStoreManager.class).to(DefaultSecureStoreService.class);
    bind(SecureStoreService.class).to(DefaultSecureStoreService.class);
    expose(SecureStore.class);
    expose(SecureStoreManager.class);
    expose(SecureStoreService.class);
  }

  /**
   * Secure store service provider.
   *
   * file binds to FileSecureStoreService
   * kms  binds to KMSSecureStoreService
   * ext  binds to SecretManagerSecureStoreService
   * none binds to DummySecureStoreService
   *
   * TODO: simplify these bindings after CDAP-14668 is fixed.
   */
  @Singleton
  private static final class SecureStoreServiceProvider implements Provider<SecureStoreService> {
    private final CConfiguration cConf;
    private final SConfiguration sConf;
    private final Injector injector;

    @Inject
    private SecureStoreServiceProvider(final CConfiguration cConf, SConfiguration sConf, Injector injector) {
      this.cConf = cConf;
      this.sConf = sConf;
      this.injector = injector;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SecureStoreService get() {
      if (SecureStoreUtils.isFileBacked(cConf)) {
        if (Strings.isNullOrEmpty(sConf.get(Constants.Security.Store.FILE_PASSWORD))) {
          throw new IllegalArgumentException("File secure store password is not set. Please set the " +
                                               "\"security.store.file.password\" property in cdap-security.xml.");
        }
        return injector.getInstance(FileSecureStoreService.class);
      }

      if (SecureStoreUtils.isKMSBacked(cConf)) {
        if (!SecureStoreUtils.isKMSCapable()) {
          throw new IllegalArgumentException("Could not find classes such as " +
                                               "org.apache.hadoop.crypto.key.kms.KMSClientProvider. KMS based secure " +
                                               "store is only supported in Apache Hadoop 2.6.0 and above.");
        }
        return injector.getInstance(SecureStoreUtils.getKMSSecureStore());
      }

      if (SecureStoreUtils.isNone(cConf)) {
        return injector.getInstance(DummySecureStoreService.class);
      }

      return injector.getInstance(SecretManagerSecureStoreService.class);
    }
  }
}
