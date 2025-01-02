/*
 * Copyright © 2024 Cask Data, Inc.
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
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.api.exception.WrappedStageException;
import io.cdap.cdap.common.conf.Constants.Logging;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.logging.context.WorkerLoggingContext;
import io.cdap.cdap.logging.filter.Filter;
import io.cdap.cdap.logging.filter.FilterParser;
import io.cdap.cdap.logging.framework.local.LocalLogAppender;
import io.cdap.cdap.logging.read.FileLogReader;
import io.cdap.cdap.logging.read.LogEvent;
import io.cdap.cdap.logging.read.LogOffset;
import io.cdap.cdap.logging.read.ReadRange;
import java.util.Map;
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

/**
 * This test class verifies that error classification tags are correctly added to log events when
 * exceptions of type {@link ProgramFailureException} and {@link WrappedStageException} are thrown.
 */
public class ErrorClassificationLoggingTest {
  private static Injector injector;
  private static TransactionManager txManager;

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void setUpContext() throws Exception {
    injector = LoggingTester.createInjector(TMP_FOLDER);
    txManager = LoggingTester.createTransactionManager(injector);
  }

  @Test
  public void testErrorClassificationTagsArePresentInProgramFailureException() {
    LogAppender appender = injector.getInstance(LocalLogAppender.class);
    new LogAppenderInitializer(appender).initialize("ErrorClassificationLoggingTest");
    Logger logger = LoggerFactory.getLogger("ErrorClassificationLoggingTest");

    LoggingContext context = new WorkerLoggingContext("namespace", "app",
        "workerid", "runid", null);
    LoggingContextAccessor.setLoggingContext(context);
    long fromTimeMillis = System.currentTimeMillis();
    logger.error("Some message", new WrappedStageException(
        new ProgramFailureException.Builder()
            .withErrorCategory(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN))
            .withErrorReason("error Reason")
            .withErrorType(ErrorType.USER)
            .withDependency(true)
            .withErrorCodeType(ErrorCodeType.HTTP)
            .withErrorCode("403")
            .withSupportedDocumentationUrl("http://www.example.com")
            .build(), "stageName"));
    appender.stop();

    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    Filter filter = FilterParser.parse("");
    ReadRange readRange = new ReadRange(fromTimeMillis, new DateTime().getMillis(),
        LogOffset.INVALID_KAFKA_OFFSET);

    CloseableIterator<LogEvent> logIter = logReader.getLog(context, readRange.getFromMillis(),
        readRange.getToMillis(), filter);

    LogEvent logEvent = logIter.next();
    Map<String, String> mdc = logEvent.getLoggingEvent().getMDCPropertyMap();
    String failedStage = mdc.get(Logging.TAG_FAILED_STAGE);
    String errorCategory = mdc.get(Logging.TAG_ERROR_CATEGORY);
    String errorReason = mdc.get(Logging.TAG_ERROR_REASON);
    String errorType = mdc.get(Logging.TAG_ERROR_TYPE);
    String dependency = mdc.get(Logging.TAG_DEPENDENCY);
    String errorCodeType = mdc.get(Logging.TAG_ERROR_CODE_TYPE);
    String errorCode = mdc.get(Logging.TAG_ERROR_CODE);
    String supportedDocumentationUrl = mdc.get(Logging.TAG_SUPPORTED_DOC_URL);
    Assert.assertEquals("stageName", failedStage);
    Assert.assertEquals("Plugin", errorCategory);
    Assert.assertEquals("error Reason", errorReason);
    Assert.assertEquals("USER", errorType);
    Assert.assertEquals(Boolean.TRUE.toString(), dependency);
    Assert.assertEquals("HTTP", errorCodeType);
    Assert.assertEquals("403", errorCode);
    Assert.assertEquals("http://www.example.com", supportedDocumentationUrl);
  }

  @Test
  public void testErrorClassificationTagsArePresentWithWrappedStageException() {
    LogAppender appender = injector.getInstance(LocalLogAppender.class);
    new LogAppenderInitializer(appender).initialize("ErrorClassificationLoggingTest");
    Logger logger = LoggerFactory.getLogger("ErrorClassificationLoggingTest");

    LoggingContext context = new WorkerLoggingContext("namespace", "app",
        "workerid", "runid", null);
    LoggingContextAccessor.setLoggingContext(context);
    long fromTimeMillis = System.currentTimeMillis();
    logger.error("Some message", new WrappedStageException(null, "stageName"));
    appender.stop();

    FileLogReader logReader = injector.getInstance(FileLogReader.class);
    Filter filter = FilterParser.parse("");
    ReadRange readRange = new ReadRange(fromTimeMillis, new DateTime().getMillis(),
        LogOffset.INVALID_KAFKA_OFFSET);

    CloseableIterator<LogEvent> logIter = logReader.getLog(context, readRange.getFromMillis(),
        readRange.getToMillis(), filter);
    LogEvent logEvent = logIter.next();
    Map<String, String> mdc = logEvent.getLoggingEvent().getMDCPropertyMap();
    String failedStage = mdc.get(Logging.TAG_FAILED_STAGE);
    String errorCategory = mdc.get(Logging.TAG_ERROR_CATEGORY);
    String errorReason = mdc.get(Logging.TAG_ERROR_REASON);
    String errorType = mdc.get(Logging.TAG_ERROR_TYPE);
    Assert.assertEquals("stageName", failedStage);
    Assert.assertEquals("Plugin", errorCategory);
    Assert.assertEquals("Stage 'stageName' encountered : null", errorReason);
    Assert.assertEquals("UNKNOWN", errorType);
  }

  @AfterClass
  public static void cleanUp() {
    txManager.stopAndWait();
  }
}
