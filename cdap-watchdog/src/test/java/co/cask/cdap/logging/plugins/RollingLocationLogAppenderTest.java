/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.plugins;

import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.util.StatusPrinter;
import co.cask.cdap.api.logging.AppenderContext;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.NoOpOwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.logging.ApplicationLoggingContext;
import co.cask.cdap.common.logging.NamespaceLoggingContext;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.context.MapReduceLoggingContext;
import co.cask.cdap.logging.framework.LocalAppenderContext;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.security.impersonation.UGIProvider;
import co.cask.cdap.security.impersonation.UnsupportedUGIProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.MDC;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Unit test for {@link RollingLocationLogAppender}
 */
public class RollingLocationLogAppenderTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" +
      RollingLocationLogAppender.class.getSimpleName();
    cConf.set(LoggingConfiguration.LOG_BASE_DIR, logBaseDir);

    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new NonCustomLocationUnitTestModule().getModule(),
      new TransactionModules().getInMemoryModules(),
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
          bind(NamespaceQueryAdmin.class).to(SimpleNamespaceQueryAdmin.class);
          bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testRollingLocationLogAppender() throws Exception {
    // assume SLF4J is bound to logback in the current environment
    AppenderContext appenderContext = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                               injector.getInstance(TransactionSystemClient.class),
                                                               injector.getInstance(LocationFactory.class),
                                                               new NoOpMetricsCollectionService());

    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(appenderContext);
    // Call context.reset() to clear any previous configuration, e.g. default
    // configuration. For multi-step configuration, omit calling context.reset().
    appenderContext.reset();

    configurator.doConfigure(getClass().getResourceAsStream("/rolling-appender-logback-test.xml"));
    StatusPrinter.printInCaseOfErrorsOrWarnings(appenderContext);

    RollingLocationLogAppender rollingAppender =
      (RollingLocationLogAppender) appenderContext.getLogger(RollingLocationLogAppenderTest.class)
        .getAppender("rollingAppender");

    addTagsToMdc("testNamespace", "testApp");
    Logger logger = appenderContext.getLogger(RollingLocationLogAppenderTest.class);
    ingestLogs(logger, 5);
    Map<LocationIdentifier, LocationOutputStream> activeFiles = rollingAppender.getLocationManager()
      .getActiveLocations();
    Assert.assertEquals(1, activeFiles.size());
    verifyFileOutput(activeFiles, 5);

    // different program should go to different directory
    addTagsToMdc("testNamespace", "testApp1");
    ingestLogs(logger, 5);
    activeFiles = rollingAppender.getLocationManager().getActiveLocations();
    Assert.assertEquals(2, activeFiles.size());
    verifyFileOutput(activeFiles, 5);

    // different program should go to different directory because namespace is different
    addTagsToMdc("testNamespace1", "testApp1");
    ingestLogs(logger, 5);
    activeFiles = rollingAppender.getLocationManager().getActiveLocations();
    Assert.assertEquals(3, activeFiles.size());
    verifyFileOutput(activeFiles, 5);
  }

  @Test
  public void testRollOver() throws Exception {
    // assume SLF4J is bound to logback in the current environment
    AppenderContext appenderContext = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                               injector.getInstance(TransactionSystemClient.class),
                                                               injector.getInstance(LocationFactory.class),
                                                               new NoOpMetricsCollectionService());

    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(appenderContext);
    // Call context.reset() to clear any previous configuration, e.g. default
    // configuration. For multi-step configuration, omit calling context.reset().
    appenderContext.reset();

    configurator.doConfigure(getClass().getResourceAsStream("/rolling-appender-logback-test.xml"));
    StatusPrinter.printInCaseOfErrorsOrWarnings(appenderContext);

    RollingLocationLogAppender rollingAppender =
      (RollingLocationLogAppender) appenderContext.getLogger(RollingLocationLogAppenderTest.class)
        .getAppender("rollingAppender");

    addTagsToMdc("testNs", "testApp");
    Logger logger = appenderContext.getLogger(RollingLocationLogAppenderTest.class);
    ingestLogs(logger, 20000);
    Map<LocationIdentifier, LocationOutputStream> activeFiles = rollingAppender.getLocationManager()
      .getActiveLocations();
    Assert.assertEquals(1, activeFiles.size());
    LocationOutputStream locationOutputStream = activeFiles.get(new LocationIdentifier("testNs", "testApp"));
    Location parentDir = Locations.getParent(locationOutputStream.getLocation());
    Assert.assertEquals(10, parentDir.list().size());

    // different program should go to different directory
    addTagsToMdc("testNs", "testApp1");
    ingestLogs(logger, 20000);
    activeFiles = rollingAppender.getLocationManager().getActiveLocations();
    Assert.assertEquals(2, activeFiles.size());
    locationOutputStream = activeFiles.get(new LocationIdentifier("testNs", "testApp1"));
    parentDir = Locations.getParent(locationOutputStream.getLocation());
    Assert.assertEquals(10, parentDir.list().size());

    // different program should go to different directory because namespace is different
    addTagsToMdc("testNs1", "testApp1");
    ingestLogs(logger, 20000);
    activeFiles = rollingAppender.getLocationManager().getActiveLocations();
    Assert.assertEquals(3, activeFiles.size());
    locationOutputStream = activeFiles.get(new LocationIdentifier("testNs1", "testApp1"));
    parentDir = Locations.getParent(locationOutputStream.getLocation());
    Assert.assertEquals(10, parentDir.list().size());
  }

  @Test
  public void testFileClose() throws Exception {
    // assume SLF4J is bound to logback in the current environment
    AppenderContext appenderContext = new LocalAppenderContext(injector.getInstance(DatasetFramework.class),
                                                               injector.getInstance(TransactionSystemClient.class),
                                                               injector.getInstance(LocationFactory.class),
                                                               new NoOpMetricsCollectionService());

    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(appenderContext);
    // Call context.reset() to clear any previous configuration, e.g. default
    // configuration. For multi-step configuration, omit calling context.reset().
    appenderContext.reset();

    configurator.doConfigure(getClass().getResourceAsStream("/rolling-appender-logback-test.xml"));
    StatusPrinter.printInCaseOfErrorsOrWarnings(appenderContext);

    RollingLocationLogAppender rollingAppender =
      (RollingLocationLogAppender) appenderContext.getLogger(RollingLocationLogAppenderTest.class)
        .getAppender("rollingAppender");

    addTagsToMdc("testNs", "testApp");
    Logger logger = appenderContext.getLogger(RollingLocationLogAppenderTest.class);
    ingestLogs(logger, 20);

    // wait for 500 ms so that file is eligible for closing
    Thread.sleep(500);
    // flush to make sure file is closed
    rollingAppender.flush();
    Assert.assertEquals(0, rollingAppender.getLocationManager().getActiveLocations().size());
  }

  private void ingestLogs(Logger logger, int entries) {
    for (int i = 0; i < entries; i++) {
      logger.info("Testing Application log: " + i);
    }
  }

  private void verifyFileOutput(Map<LocationIdentifier, LocationOutputStream> activeFilesToLocation, int entries) throws
    IOException {
    for (LocationOutputStream locationOutputStream : activeFilesToLocation.values()) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        locationOutputStream.getLocation().getInputStream(), "UTF-8"));

      int count = 0;
      while ((bufferedReader.readLine()) != null) {
        count++;
      }
      Assert.assertEquals(entries, count);
    }
  }

  private void addTagsToMdc(String namespace, String applicationName) {
    MDC.put(NamespaceLoggingContext.TAG_NAMESPACE_ID, namespace);
    MDC.put(ApplicationLoggingContext.TAG_APPLICATION_ID, applicationName);
    MDC.put(FlowletLoggingContext.TAG_FLOW_ID, "testFlow");
    MDC.put(FlowletLoggingContext.TAG_FLOWLET_ID, "testFlowet");
    MDC.put(MapReduceLoggingContext.TAG_MAP_REDUCE_JOB_ID, "testMapRed1");
    MDC.put(MapReduceLoggingContext.TAG_INSTANCE_ID, "testMapRedInst1");
    MDC.put(MapReduceLoggingContext.TAG_RUN_ID, "testMapRedRunId1");
  }
}
