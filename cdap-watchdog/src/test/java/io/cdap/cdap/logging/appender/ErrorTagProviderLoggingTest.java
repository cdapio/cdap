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

package io.cdap.cdap.logging.appender;

import com.google.inject.Injector;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.error.api.ErrorTagProvider;
import io.cdap.cdap.error.api.ErrorTagProvider.ErrorTag;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.ReadRange;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.tephra.TransactionManager;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorTagProviderLoggingTest {
  private static Injector injector;
  private static TransactionManager txManager;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setUpContext() throws Exception {
    injector = LoggingTester.createInjector(TMP_FOLDER);
    txManager = LoggingTester.createTransactionManager(injector);
  }

  public class TestErrorTagProviderException extends RuntimeException implements ErrorTagProvider {
    final Set<ErrorTag> errorTags = new HashSet<>();

    TestErrorTagProviderException(Throwable cause, ErrorTag... tags) {
      super(cause);
      errorTags.addAll(Arrays.asList(tags));
    }

    @Override
    public Set<ErrorTag> getErrorTags() {
      return errorTags;
    }
  }

  @Test
  public void testErrorCodeProviderException() {
    LogAppender appender = injector.getInstance(LocalLogAppender.class);
    new LogAppenderInitializer(appender).initialize("ErrorCodeProviderLoggingTest");
    Logger logger = LoggerFactory.getLogger("ErrorCodeProviderLoggingTest");

    LoggingContext context = new WorkerLoggingContext("namespace", "app", "workerid", "runid", null);
    LoggingContextAccessor.setLoggingContext(context);
    logger.error("Some message", new TestErrorTagProviderException(new TestErrorTagProviderException(null,
      ErrorTag.DEPENDENCY, ErrorTag.PLUGIN), ErrorTag.SYSTEM, ErrorTag.PLUGIN));
    appender.stop();

    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    Filter filter = FilterParser.parse("");
    ReadRange readRange = new ReadRange(0, new DateTime().getMillis(),
      LogOffset.INVALID_KAFKA_OFFSET);

    CloseableIterator<LogEvent> logIter = logReader.getLog(context, readRange.getFromMillis(),
      readRange.getToMillis(), filter);

    LogEvent logEvent = logIter.next();
    Map<String, String> mdc = logEvent.getLoggingEvent().getMDCPropertyMap();
    String errorTagsStr = mdc.get(LogAppender.ERROR_TAGS);
    Assert.assertTrue(errorTagsStr.contains(ErrorTag.DEPENDENCY.toString()));
    Assert.assertTrue(errorTagsStr.contains(ErrorTag.SYSTEM.toString()));
    Assert.assertTrue(errorTagsStr.contains(ErrorTag.PLUGIN.toString()));
    // assert plugin is not duplicated
    Assert.assertEquals(3, errorTagsStr.split(",").length);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    txManager.stopAndWait();
  }

}
