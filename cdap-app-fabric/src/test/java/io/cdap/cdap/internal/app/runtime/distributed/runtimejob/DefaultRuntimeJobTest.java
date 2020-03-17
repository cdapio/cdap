/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.runtimejob;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.KeyStores;
import io.cdap.cdap.common.twill.NoopTwillRunnerService;
import io.cdap.cdap.logging.appender.LogAppenderInitializer;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Unit test for {@link DefaultRuntimeJob}.
 */
@RunWith(Parameterized.class)
public class DefaultRuntimeJobTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Parameterized.Parameters(name = "{index}: DefaultRuntimeJobTest(activeMonitoring = {0})")
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {false},
      {true}
    });
  }

  private final boolean activeMonitoring;

  public DefaultRuntimeJobTest(boolean activeMonitoring) {
    this.activeMonitoring = activeMonitoring;
  }

  @Test
  public void testInjector() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().toString());
    cConf.setBoolean(Constants.RuntimeMonitor.ACTIVE_MONITORING, activeMonitoring);

    File serverKeyStoreFile = TEMP_FOLDER.newFile();
    try (OutputStream os = new FileOutputStream(serverKeyStoreFile)) {
      KeyStores.generatedCertKeyStore(1, "").store(os, "".toCharArray());
    }
    cConf.set(Constants.RuntimeMonitor.SERVER_KEYSTORE_PATH, serverKeyStoreFile.getAbsolutePath());

    File clientKeyStoreFile = TEMP_FOLDER.newFile();
    try (OutputStream os = new FileOutputStream(clientKeyStoreFile)) {
      KeyStores.generatedCertKeyStore(1, "").store(os, "".toCharArray());
    }
    cConf.set(Constants.RuntimeMonitor.CLIENT_KEYSTORE_PATH, clientKeyStoreFile.getAbsolutePath());

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFile());

    DefaultRuntimeJob defaultRuntimeJob = new DefaultRuntimeJob();
    Injector injector = Guice.createInjector(defaultRuntimeJob.createModules(new RuntimeJobEnvironment() {

      @Override
      public LocationFactory getLocationFactory() {
        return locationFactory;
      }

      @Override
      public TwillRunner getTwillRunner() {
        return new NoopTwillRunnerService();
      }

      @Override
      public Map<String, String> getProperties() {
        return Collections.emptyMap();
      }
    }, cConf));

    injector.getInstance(LogAppenderInitializer.class);
    defaultRuntimeJob.createCoreServices(injector);
  }
}
