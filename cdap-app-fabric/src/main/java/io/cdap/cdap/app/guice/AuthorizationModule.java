/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package io.cdap.cdap.app.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.data.dataset.SystemDatasetInstantiator;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.dataset2.DynamicDatasetCache;
import io.cdap.cdap.data2.dataset2.MultiThreadDatasetCache;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.runtime.DefaultAdmin;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.authorization.AccessControllerInstantiator;
import io.cdap.cdap.security.authorization.AuthorizationContextFactory;
import io.cdap.cdap.security.authorization.AuthorizerInstantiator;
import io.cdap.cdap.security.authorization.DefaultAuthorizationContext;
import io.cdap.cdap.security.authorization.DelegatingPrivilegeManager;
import io.cdap.cdap.security.spi.authorization.AuthorizationContext;
import io.cdap.cdap.security.spi.authorization.Authorizer;
import io.cdap.cdap.security.spi.authorization.PrivilegesManager;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionSystemClient;

import java.util.Properties;

/**
 * {@link PrivateModule} for authorization classes. This module is necessary and must be in app-fabric because classes
 * like {@link DatasetFramework}, {@link DynamicDatasetCache}, {@link DefaultAdmin} are not available in cdap-security.
 *
 * This module is part of the injector created in StandaloneMain and MasterServiceMain, which makes it available to
 * services. The requirements for this module are:
 * 1. This module is used for creating and exposing {@link AuthorizerInstantiator}.
 * 2. The {@link AuthorizerInstantiator} needs a {@link DefaultAuthorizationContext}.
 * 3. The {@link DefaultAuthorizationContext} needs a {@link DatasetContext}, a {@link Admin} and a
 * {@link Transactional}.
 *
 * These requirements are fulfilled by:
 * 1. Constructing a {@link Singleton} {@link MultiThreadDatasetCache} by injecting a {@link DatasetFramework}, a
 * {@link TransactionSystemClient} and a {@link MetricsCollectionService}. This {@link MultiThreadDatasetCache} is
 * created for datasets in the {@link NamespaceId#SYSTEM}, since the datasets that {@link Authorizer} extensions need
 * are created in the system namespace.
 * 2. Binding the {@link DatasetContext} to the {@link MultiThreadDatasetCache} created above.
 * 3. Using the {@link MultiThreadDatasetCache} to create a {@link TransactionContext} for providing the
 * {@link Transactional}.
 * 4. Binding a {@link DefaultAdmin} by injecting {@link DatasetFramework}. This {@link DefaultAdmin} is also created
 * for the {@link NamespaceId#SYSTEM}.
 * 5. Using the bound {@link DatasetContext}, {@link Admin} and {@link Transactional} to provide the injections for
 * {@link DefaultAuthorizationContext}, which is provided using a {@link Guice} {@link FactoryModuleBuilder} to
 * construct a {@link AuthorizationContextFactory}.
 * 6. Only exposing a {@link Singleton} binding to {@link AuthorizerInstantiator} from this module. The
 * {@link AuthorizerInstantiator} can just {@link Inject} the {@link AuthorizationContextFactory} and call
 * {@link AuthorizationContextFactory#create(Properties)} using an {@link Assisted} {@link Properties} object.
 */
public class AuthorizationModule extends PrivateModule {
  @Override
  protected void configure() {
    bind(DynamicDatasetCache.class).toProvider(DynamicDatasetCacheProvider.class).in(Scopes.SINGLETON);
    bind(DatasetContext.class).to(DynamicDatasetCache.class).in(Scopes.SINGLETON);
    bind(Admin.class).toProvider(AdminProvider.class);
    bind(Transactional.class).toProvider(TransactionalProvider.class);

    install(
      new FactoryModuleBuilder()
        .implement(AuthorizationContext.class, DefaultAuthorizationContext.class)
        .build(AuthorizationContextFactory.class)
    );

    bind(AuthorizerInstantiator.class).in(Scopes.SINGLETON);
    expose(AuthorizerInstantiator.class);
    bind(AccessControllerInstantiator.class).in(Scopes.SINGLETON);
    expose(AccessControllerInstantiator.class);

    bind(PrivilegesManager.class).to(DelegatingPrivilegeManager.class);
    expose(PrivilegesManager.class);
  }

  @Singleton
  private static final class DynamicDatasetCacheProvider implements Provider<DynamicDatasetCache> {

    private final DatasetFramework dsFramework;
    private final TransactionSystemClient txClient;
    private final MetricsCollectionService metricsCollectionService;

    @Inject
    private DynamicDatasetCacheProvider(DatasetFramework dsFramework, TransactionSystemClient txClient,
                                        MetricsCollectionService metricsCollectionService) {
      this.dsFramework = dsFramework;
      this.txClient = txClient;
      this.metricsCollectionService = metricsCollectionService;
    }

    @Override
    public DynamicDatasetCache get() {
      SystemDatasetInstantiator dsInstantiator = new SystemDatasetInstantiator(dsFramework);
      return new MultiThreadDatasetCache(
        dsInstantiator, txClient, NamespaceId.SYSTEM, ImmutableMap.of(),
        metricsCollectionService.getContext(ImmutableMap.of()),
        ImmutableMap.of()
      );
    }
  }

  @Singleton
  private static final class AdminProvider implements Provider<Admin> {
    private final DatasetFramework dsFramework;
    private final SecureStoreManager secureStoreManager;
    private final NamespaceQueryAdmin namespaceQueryAdmin;

    @Inject
    private AdminProvider(DatasetFramework dsFramework, SecureStoreManager secureStoreManager,
                          NamespaceQueryAdmin namespaceQueryAdmin) {
      this.dsFramework = dsFramework;
      this.secureStoreManager = secureStoreManager;
      this.namespaceQueryAdmin = namespaceQueryAdmin;
    }

    @Override
    public Admin get() {
      return new DefaultAdmin(dsFramework, NamespaceId.SYSTEM, secureStoreManager, namespaceQueryAdmin);
    }
  }

  @Singleton
  private static final class TransactionalProvider implements Provider<Transactional> {

    private final DynamicDatasetCache datasetCache;

    @Inject
    private TransactionalProvider(DynamicDatasetCache datasetCache) {
      this.datasetCache = datasetCache;
    }

    @Override
    public Transactional get() {
      return Transactions.createTransactional(datasetCache);
    }
  }
}
