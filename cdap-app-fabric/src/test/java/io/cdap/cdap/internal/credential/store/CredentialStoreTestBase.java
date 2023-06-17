/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.credential.store;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.store.StoreDefinition;
import java.io.IOException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CredentialStoreTestBase {

  private static TransactionManager txManager;
  static CredentialProfileStore credentialProfileStore;
  static CredentialIdentityStore credentialIdentityStore;

  @BeforeClass
  public static void beforeClass() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    Injector injector = Guice.createInjector(new ConfigModule(cConf),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new StorageModule(),
        new TransactionModules().getInMemoryModules(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        });
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    TransactionRunner runner = injector.getInstance(TransactionRunner.class);
    StoreDefinition.CredentialProvisionerStore.create(injector
        .getInstance(StructuredTableAdmin.class));
    credentialProfileStore = new CredentialProfileStore(runner);
    credentialIdentityStore = new CredentialIdentityStore(runner);
  }

  @AfterClass
  public static void teardown() throws Exception {
    if (txManager != null) {
      txManager.stopAndWait();
    }
  }
}
