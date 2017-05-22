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

package co.cask.cdap.app.guice;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.DefaultAdmin;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.authorization.AuthorizationContextFactory;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.cdap.security.authorization.DefaultAuthorizationContext;
import co.cask.cdap.security.authorization.DefaultPrivilegesManager;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionSystemClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
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

    bind(PrivilegesManager.class).to(DefaultPrivilegesManager.class);
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
        dsInstantiator, txClient, NamespaceId.SYSTEM, ImmutableMap.<String, String>of(),
        metricsCollectionService.getContext(ImmutableMap.<String, String>of()),
        ImmutableMap.<String, Map<String, String>>of()
      );
    }
  }

  @Singleton
  private static final class AdminProvider implements Provider<Admin> {
    private final DatasetFramework dsFramework;
    private final SecureStoreManager secureStoreManager;

    @Inject
    private AdminProvider(DatasetFramework dsFramework, SecureStoreManager secureStoreManager) {
      this.dsFramework = dsFramework;
      this.secureStoreManager = secureStoreManager;
    }

    @Override
    public Admin get() {
      return new DefaultAdmin(dsFramework, NamespaceId.SYSTEM, secureStoreManager);
    }
  }

  @Singleton
  private static final class TransactionalProvider implements Provider<Transactional> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionalProvider.class);

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
