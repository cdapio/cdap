/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering.runtime.spi.provisioner;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.auditlogging.AuditLogWriter;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.app.runtime.NoOpProgramStateWriter;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.internal.provision.ProvisionerConfig;
import io.cdap.cdap.internal.provision.ProvisionerConfigProvider;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.internal.provision.ProvisionerProvider;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.security.FakeSecureStore;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import java.util.Map;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TetheringProvisionerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testGuiceInjector() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new LocalLocationModule(),
      new InMemoryDiscoveryModule(),
      new StorageModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new ProvisionerModule(),
      new AuthorizationEnforcementModule().getNoOpModules(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(SecureStore.class).toInstance(FakeSecureStore.builder().build());
          bind(ProgramStateWriter.class).to(NoOpProgramStateWriter.class);
          bind(AuditLogWriter.class).toInstance(auditLogContexts -> {});
        }
      }
    );

    ProvisionerProvider provisionerProvider = injector.getInstance(ProvisionerProvider.class);
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Assert.assertNotNull(provisioners.get(TetheringProvisioner.TETHERING_NAME));

    ProvisionerConfigProvider configProvider = injector.getInstance(ProvisionerConfigProvider.class);
    Map<String, ProvisionerConfig> configs = configProvider.loadProvisionerConfigs(provisioners.values());
    Assert.assertNotNull(configs.get(TetheringProvisioner.TETHERING_NAME));
  }
}
