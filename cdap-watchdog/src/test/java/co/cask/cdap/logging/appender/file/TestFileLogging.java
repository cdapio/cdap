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
import co.cask.cdap.common.guice.NonCustomLocationUnitTestModule;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.NoOpMetricsCollectionService;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.SimpleNamespaceQueryAdmin;
import co.cask.cdap.common.security.UGIProvider;
import co.cask.cdap.common.security.UnsupportedUGIProvider;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.appender.LogAppender;
import co.cask.cdap.logging.appender.LogAppenderInitializer;
import co.cask.cdap.logging.appender.LoggingTester;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.logging.filter.Filter;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.logging.read.FileLogReader;
import co.cask.cdap.logging.read.LogEvent;
import co.cask.cdap.logging.read.ReadRange;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import com.google.common.collect.Lists;
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

import java.util.List;

/**
 * Test logging to Avro file.
 */
public class TestFileLogging {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static Injector injector;
  private static TransactionManager txManager;

  @BeforeClass
  public static void setUpContext() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());
    cConf.setInt(LoggingConfiguration.LOG_MAX_FILE_SIZE_BYTES, 20 * 1024);
    String logBaseDir = cConf.get(LoggingConfiguration.LOG_BASE_DIR) + "/" + TestFileLogging.class.getSimpleName();
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
        }
      }
    );

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    LogAppender appender = injector.getInstance(FileLogAppender.class);
    new LogAppenderInitializer(appender).initialize("TestFileLogging");

    Logger logger = LoggerFactory.getLogger("TestFileLogging");
    LoggingTester loggingTester = new LoggingTester();
    loggingTester.generateLogs(logger, new FlowletLoggingContext("TFL_NS_1", "APP_1", "FLOW_1", "FLOWLET_1",
                                                                 "RUN1", "INSTANCE1"));
    appender.stop();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

  @Test
  public void testGetLogNext() throws Exception {
    LoggingContext loggingContext = new FlowletLoggingContext("TFL_NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE1");
    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    LoggingTester tester = new LoggingTester();
    tester.testGetNext(logReader, loggingContext);
  }

  @Test
  public void testGetLogPrev() throws Exception {
    LoggingContext loggingContext = new FlowletLoggingContext("TFL_NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE1");
    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    LoggingTester tester = new LoggingTester();
    tester.testGetPrev(logReader, loggingContext);
  }

  @Test
  public void testGetLog() throws Exception {
    // LogReader.getLog is tested in LogSaverTest for distributed mode
    LoggingContext loggingContext = new FlowletLoggingContext("TFL_NS_1", "APP_1", "FLOW_1", "", "RUN1", "INSTANCE1");
    FileLogReader logTail = injector.getInstance(FileLogReader.class);
    LoggingTester.LogCallback logCallback1 = new LoggingTester.LogCallback();
    logTail.getLogPrev(loggingContext, ReadRange.LATEST, 60, Filter.EMPTY_FILTER,
                       logCallback1);
    List<LogEvent> allEvents = logCallback1.getEvents();
    Assert.assertEquals(60, allEvents.size());

    List<LogEvent> events =
      Lists.newArrayList(logTail.getLog(loggingContext, allEvents.get(10).getLoggingEvent().getTimeStamp(),
                                        allEvents.get(15).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));

    Assert.assertEquals(5, events.size());
    Assert.assertEquals(allEvents.get(10).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(14).getLoggingEvent().getFormattedMessage(),
                        events.get(4).getLoggingEvent().getFormattedMessage());


    events =
      Lists.newArrayList(logTail.getLog(loggingContext, allEvents.get(0).getLoggingEvent().getTimeStamp(),
                                        allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(59, events.size());
    Assert.assertEquals(allEvents.get(0).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(58).getLoggingEvent().getFormattedMessage());

    events =
      Lists.newArrayList(logTail.getLog(loggingContext, allEvents.get(12).getLoggingEvent().getTimeStamp(),
                                        allEvents.get(41).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(29, events.size());
    Assert.assertEquals(allEvents.get(12).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(40).getLoggingEvent().getFormattedMessage(),
                        events.get(28).getLoggingEvent().getFormattedMessage());

    events =
      Lists.newArrayList(logTail.getLog(loggingContext, allEvents.get(22).getLoggingEvent().getTimeStamp(),
                                        allEvents.get(38).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(16, events.size());
    Assert.assertEquals(allEvents.get(22).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(37).getLoggingEvent().getFormattedMessage(),
                        events.get(15).getLoggingEvent().getFormattedMessage());

    events =
      Lists.newArrayList(logTail.getLog(loggingContext, allEvents.get(41).getLoggingEvent().getTimeStamp(),
                                        allEvents.get(59).getLoggingEvent().getTimeStamp(), Filter.EMPTY_FILTER));
    Assert.assertEquals(18, events.size());
    Assert.assertEquals(allEvents.get(41).getLoggingEvent().getFormattedMessage(),
                        events.get(0).getLoggingEvent().getFormattedMessage());
    Assert.assertEquals(allEvents.get(58).getLoggingEvent().getFormattedMessage(),
                        events.get(17).getLoggingEvent().getFormattedMessage());

    // Try with null run id, should get all logs for FLOW_1
    LoggingContext loggingContext1 = new FlowletLoggingContext("TFL_NS_1", "APP_1", "FLOW_1", "", null, "INSTANCE1");
    events =
      Lists.newArrayList(logTail.getLog(loggingContext1, 0, Long.MAX_VALUE, Filter.EMPTY_FILTER));
    Assert.assertEquals(100, events.size());
  }
}
