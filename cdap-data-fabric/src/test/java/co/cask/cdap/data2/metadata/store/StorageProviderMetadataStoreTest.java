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

package co.cask.cdap.data2.metadata.store;

import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.guice.NamespaceAdminTestModule;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.audit.AuditTestModule;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.dataset.DatasetMetadataStorage;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Tests for the {@link MetadataStore} that delegates to the metadata storage provider.
 */
public class StorageProviderMetadataStoreTest extends AbstractMetadataStoreTest {

  private static TransactionManager txManager;

  @BeforeClass
  public static void setupDefaultStore() throws IOException {
    injector = Guice.createInjector(
      new ConfigModule(),
      Modules.override(
        new DataSetsModules().getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          // Need the distributed metadata store.
          bind(MetadataStore.class).to(StorageProviderMetadataStore.class);
          bind(MetadataStorage.class).to(DatasetMetadataStorage.class).in(Scopes.SINGLETON);
        }
      }),
      new LocalLocationModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new StorageModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new AuditTestModule()
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    AbstractMetadataStoreTest.commonSetup();
  }

  @AfterClass
  public static void teardown() throws IOException {
    try {
      AbstractMetadataStoreTest.commonTearDown();
    } finally {
      txManager.stopAndWait();
    }
  }
}

