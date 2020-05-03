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

package io.cdap.cdap.logging.appender;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.logging.logback.TestLoggingContext;
import io.cdap.cdap.common.utils.Tasks;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class LogAppenderInitializerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test (timeout = 10000L)
  public void testConfigUpdate() throws Exception {
    File logbackFile = createLogbackFile(new File(TEMP_FOLDER.newFolder(), "logback.xml"), "");

    // Create a logger context from the generated logback.xml file
    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
    loggerContext.reset();

    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(loggerContext);
    configurator.doConfigure(logbackFile);

    TestLogAppender testAppender = new TestLogAppender();
    testAppender.setName("TestAppender");
    LogAppenderInitializer initializer = new LogAppenderInitializer(testAppender);
    initializer.initialize();

    LoggingContextAccessor.setLoggingContext(new TestLoggingContext("ns", "app", "run", "instance"));

    Logger logger = loggerContext.getLogger(LogAppenderInitializerTest.class);
    logger.info("Testing");

    Assert.assertEquals("Testing", testAppender.getLastMessage());

    // Update the logback file
    createLogbackFile(logbackFile, "<logger name=\"io.cdap.cdap\" level=\"INFO\" />");

    // Wait till the test appender stop() is called. This will happen when logback detected the changes.
    while (!testAppender.isStoppedOnce()) {
      // We need to keep logging because there is some internal thresold in logback implementation to trigger the
      // config reload thread.
      logger.info("Waiting stop");
      TimeUnit.MILLISECONDS.sleep(150);
      // Update the last modified time of the logback file to make sure logback can pick it with.
      logbackFile.setLastModified(System.currentTimeMillis());
    }

    // The appender should get automatically started again
    Tasks.waitFor(true, testAppender::isStarted, 5, TimeUnit.SECONDS);
    logger.info("Reattached");

    Assert.assertEquals("Reattached", testAppender.getLastMessage());
    loggerContext.stop();
  }

  private File createLogbackFile(File file, String template) throws IOException {
    Path tmpFile = Files.createTempFile(file.getParentFile().toPath(), "logback", ".xml");
    URL url = getClass().getClassLoader().getResource("test-appender.xml");

    // Read in the logback xml and replace ${TEMPLATE} with the given template string.
    String content = Resources.readLines(url, StandardCharsets.UTF_8, new LineProcessor<String>() {

      private final StringBuilder builder = new StringBuilder();

      @Override
      public boolean processLine(String line) {
        builder.append(line.replace("${TEMPLATE}", template));
        return true;
      }

      @Override
      public String getResult() {
        return builder.toString();
      }
    });

    Files.delete(tmpFile);
    try (BufferedWriter writer = Files.newBufferedWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
    Files.move(tmpFile, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
    return file;
  }


  private static final class TestLogAppender extends LogAppender {

    private volatile String lastMessage;
    private volatile boolean stoppedOnce;

    @Override
    protected void appendEvent(LogMessage logMessage) {
      lastMessage = logMessage.getFormattedMessage();
    }

    @Override
    public void stop() {
      super.stop();
      this.stoppedOnce = true;
    }

    boolean isStoppedOnce() {
      return stoppedOnce;
    }

    String getLastMessage() {
      return lastMessage;
    }
  }
}
