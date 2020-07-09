/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.preview.PreviewHttpModule;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.app.preview.PreviewRequestQueue;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.provision.ProvisionerModule;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.guice.LogReaderRuntimeModules;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import io.cdap.cdap.security.impersonation.DefaultOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.impersonation.UGIProvider;
import io.cdap.cdap.security.impersonation.UnsupportedUGIProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests for {@link DefaultPreviewManager}.
 */
public class DefaultPreviewManagerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void beforeClass() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(Constants.Preview.CACHE_SIZE, 1);

    injector = Guice.createInjector(
      new ConfigModule(cConf, new Configuration()),
      new IOModule(),
      new DataFabricModules().getInMemoryModules(),
      new DataSetsModules().getStandaloneModules(),
      new TransactionExecutorModule(),
      new DataSetServiceModules().getInMemoryModules(),
      new InMemoryDiscoveryModule(),
      new AppFabricServiceRuntimeModule().getInMemoryModules(),
      new ProgramRunnerRuntimeModule().getInMemoryModules(),
      new NonCustomLocationUnitTestModule(),
      new LocalLogAppenderModule(),
      new LogReaderRuntimeModules().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new ExploreClientModule(),
      new ConfigStoreModule(),
      new MetadataServiceModule(),
      new MetadataReaderWriterModules().getInMemoryModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getStandaloneModules(),
      new SecureStoreServerModule(),
      new MessagingServerRuntimeModule().getInMemoryModules(),
      new PreviewHttpModule(),
      new PreviewConfigModule(cConf, new Configuration(), SConfiguration.create()),
      new ProvisionerModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
          bind(OwnerAdmin.class).to(DefaultOwnerAdmin.class);
          // bind PreviewRunnerStopper to Mock implementation
          bind(PreviewRunnerServiceStopper.class).toInstance(runnerId -> {

          });
        }
      }
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    txManager.stopAndWait();
  }

  private Injector getInjector() {
    return injector;
  }

  @Test
  public void testInjector() throws Exception {
    DefaultPreviewManager previewManager = (DefaultPreviewManager) getInjector().getInstance(PreviewManager.class);

    Injector previewInjector = previewManager.createPreviewInjector();
    PreviewRequestQueue queue = previewInjector.getInstance(PreviewRequestQueue.class);

    // Make sure same PreviewRunner instance is returned for a preview system
    Assert.assertEquals(queue, previewInjector.getInstance(PreviewRequestQueue.class));

    // Also make sure it can return a LogReader
    previewInjector.getInstance(LogReader.class);

    previewManager.startAndWait();
    previewManager.stopAndWait();
  }
}
