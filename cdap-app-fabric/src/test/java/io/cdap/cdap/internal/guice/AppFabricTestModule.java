/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.guice;

import com.google.inject.AbstractModule;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.MonitorHandlerModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.guice.NonCustomLocationUnitTestModule;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.config.guice.ConfigStoreModule;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.TransactionExecutorModule;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.provision.MockProvisionerModule;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.logging.guice.LogQueryServerModule;
import io.cdap.cdap.logging.guice.LogReaderRuntimeModules;
import io.cdap.cdap.messaging.guice.MessagingServerRuntimeModule;
import io.cdap.cdap.metadata.MetadataReaderWriterModules;
import io.cdap.cdap.metadata.MetadataServiceModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsHandlerModule;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.TwillRunner;

import java.io.File;
import javax.annotation.Nullable;

/**
 *
 */
public final class AppFabricTestModule extends AbstractModule {

  private final CConfiguration cConf;
  private final Configuration hConf;
  private final SConfiguration sConf;

  public AppFabricTestModule(CConfiguration configuration) {
    this(configuration, null);
  }

  public AppFabricTestModule(CConfiguration cConf, @Nullable SConfiguration sConf) {
    this.cConf = cConf;

    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));

    hConf = new Configuration();
    hConf.addResource("mapred-site-local.xml");
    hConf.reloadConfiguration();
    hConf.set("hadoop.tmp.dir", new File(localDataDir, cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsolutePath());
    hConf.set(Constants.CFG_LOCAL_DATA_DIR, localDataDir.getAbsolutePath());
    hConf.set(Constants.AppFabric.OUTPUT_DIR, cConf.get(Constants.AppFabric.OUTPUT_DIR));

    this.sConf = sConf == null ? SConfiguration.create() : sConf;
  }

  @Override
  protected void configure() {
    install(new DataFabricModules().getInMemoryModules());
    install(new DataSetsModules().getStandaloneModules());
    install(new TransactionExecutorModule());
    install(new DataSetServiceModules().getInMemoryModules());
    install(new ConfigModule(cConf, hConf, sConf));
    install(new IOModule());
    install(new InMemoryDiscoveryModule());
    install(new AppFabricServiceRuntimeModule().getInMemoryModules());
    install(new MonitorHandlerModule(false));
    install(new ProgramRunnerRuntimeModule().getInMemoryModules());
    install(new NonCustomLocationUnitTestModule());
    install(new LocalLogAppenderModule());
    install(new LogReaderRuntimeModules().getInMemoryModules());
    install(new LogQueryServerModule());
    install(new MetricsHandlerModule());
    install(new MetricsClientRuntimeModule().getInMemoryModules());
    install(new ExploreClientModule());
    install(new ConfigStoreModule());
    install(new MetadataServiceModule());
    install(new AuthorizationModule());
    install(new AuthorizationEnforcementModule().getStandaloneModules());
    install(new SecureStoreServerModule());
    install(new MetadataReaderWriterModules().getInMemoryModules());
    install(new MessagingServerRuntimeModule().getInMemoryModules());
    install(new MockProvisionerModule());
    // Needed by MonitorHandlerModuler
    bind(TwillRunner.class).to(NoopTwillRunnerService.class);
  }
}
