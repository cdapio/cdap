/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.logging.appender.file;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.DefaultNamespacedLocationFactory;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.namespace.guice.NamespaceClientRuntimeModule;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionModules;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Test for verifying that the log files for custom namespace are being created at the provided custom location
 * Note: The Guice injector here binds to the actual {@link DefaultNamespacedLocationFactory} unlike other logging
 * unit tests. This is required to test custom mapped namespaces.
 */
public class TestCustomNSLogging {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static TransactionManager txManager;
  private static NamespaceAdmin namespaceAdmin;
  private static NamespacedLocationFactory namespacedLocationFactory;
  private static LogAppender appender;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + TestCustomNSLogging.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      // here bind to the actual LocationRuntimeModule which binds the NamespacesLocationFactory to
      // DefaultNmaespacedLocationFactory which does look up to resolve namespace location mappinng
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new NamespaceClientRuntimeModule().getInMemoryModules(),
      new LoggingModules().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(UGIProvider.class).to(UnsupportedUGIProvider.class);
        }
      }
    );

    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    namespacedLocationFactory = injector.getInstance(NamespacedLocationFactory.class);
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    appender = injector.getInstance(FileLogAppender.class);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testLogFileLocation() throws Exception {
    // create a temp folder for custom ns mapping
    File customNSDir = TMP_FOLDER.newFolder("TFL_NS_1");
    NamespaceMeta namespaceMeta = new NamespaceMeta.Builder().setName("TFL_NS_1")
      .setRootDirectory(customNSDir.toString()).build();
    // create a namespace with the custom directory
    namespaceAdmin.create(namespaceMeta);

    // publish some logs
    new LogAppenderInitializer(appender).initialize("TestFileLogging");
    Logger logger = LoggerFactory.getLogger("TestFileLogging");
    LoggingTester loggingTester = new LoggingTester();
    FlowletLoggingContext loggingContext = new FlowletLoggingContext(namespaceMeta.getNamespaceId().getNamespace(),
                                                                     "APP_1", "FLOW_1", "FLOWLET_1", "RUN1",
                                                                     "INSTANCE1");
    loggingTester.generateLogs(logger, loggingContext);
    appender.stop();

    // verify that the logs directory got created at the custom location and have files in it we don't care what it
    // is and what is written here
    String logPathFragment = loggingContext.getLogPathFragment(cConf.get(LoggingConfiguration.LOG_BASE_DIR)).split
      (namespaceMeta.getName())[1];
    Assert.assertTrue(new File(customNSDir, logPathFragment).exists());
    Assert.assertTrue(new File(customNSDir, logPathFragment).list().length > 0);

    // nothing should have been created in the cdap location
    Assert.assertTrue(namespacedLocationFactory.getBaseLocation().list().isEmpty());
  }
}
