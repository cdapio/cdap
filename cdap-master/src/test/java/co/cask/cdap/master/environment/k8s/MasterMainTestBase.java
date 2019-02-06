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

package co.cask.cdap.master.environment.k8s;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.guice.MessagingServerRuntimeModule;
import co.cask.cdap.messaging.server.MessagingHttpService;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.util.function.Consumer;

/**
 * Base class to help testing various master service main classes.
 */
public class MasterMainTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  static void initialize(Consumer<CConfiguration> confUpdater) throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.clear();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    confUpdater.accept(cConf);

    // Write the "cdap-site.xml" to the "target" directory. It is determined by the current class classpath
    URL classPathURL = ClassLoaders.getClassPathURL(MessagingServiceMainTest.class);
    Assert.assertNotNull(classPathURL);
    File confDir = new File(classPathURL.getPath());
    try (Writer writer = Files.newBufferedWriter(new File(confDir, "cdap-site.xml").toPath())) {
      cConf.writeXml(writer);
    }
  }

  /**
   * Starts a new TMS service that uses the same configurations and discovery service from the given Guice
   * {@link Injector} that are constructed by master main service.
   *
   * @param mainInjector the Guice injector used by the master main service
   * @return a {@link Closeable} to shutdown the TMS
   */
  Closeable startTMS(Injector mainInjector) {
    // start TMS by sharing the same discovery service
    Injector tmsInject = Guice.createInjector(
      new ConfigModule(mainInjector.getInstance(CConfiguration.class),
                       mainInjector.getInstance(Configuration.class),
                       mainInjector.getInstance(SConfiguration.class)),
      new IOModule(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MessagingServerRuntimeModule().getStandaloneModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(DiscoveryServiceClient.class).toInstance(mainInjector.getInstance(DiscoveryServiceClient.class));
          bind(DiscoveryService.class).toInstance(mainInjector.getInstance(DiscoveryService.class));
        }
      }
    );

    MessagingService tms = tmsInject.getInstance(MessagingService.class);
    MessagingHttpService httpService = tmsInject.getInstance(MessagingHttpService.class);

    if (tms instanceof Service) {
      ((Service) tms).startAndWait();
    }
    httpService.startAndWait();

    return () -> {
      try {
        httpService.stopAndWait();
      } finally {
        if (tms instanceof Service) {
          ((Service) tms).stopAndWait();
        }
      }
    };
  }
}
