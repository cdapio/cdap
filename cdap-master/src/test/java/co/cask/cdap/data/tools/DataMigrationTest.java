/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.data.tools;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Data migration parsing tests
 */
public class DataMigrationTest {

  @Test
  public void testArgumentsParsing() throws Exception {
    // in testMigrationParse, we emit logs if there are any issues with arguments and
    // no message is emitted if the arguments are valid
    AtomicBoolean logs = resetLogging();
    // valid case
    DataMigration.testMigrationParse(new String[] {"metrics", "--keep-old-metrics-data"});
    Assert.assertFalse(resetLogging().get());

    List<String[]> validArgumentList = ImmutableList.of(new String[] {"metrics", "--keep-old-metrics-data"},
                                                        new String[] {"metrics"},
                                                        new String[] {"help"});

    List<String[]> invalidArgumentList = ImmutableList.of(new String[] {"metrics", "--keep-all-data"},
                                                          new String[] {"metrics", "-1", "-2", "-3"}
                                                          );

    // no logs on valid cases
    for (String[] arguments : validArgumentList) {
      logs = resetLogging();
      DataMigration.testMigrationParse(arguments);
      Assert.assertFalse(logs.get());
    }

    // logs with invalid cases
    for (String[] arguments : invalidArgumentList) {
      logs = resetLogging();
      DataMigration.testMigrationParse(arguments);
      Assert.assertTrue(logs.get());
    }

  }

  private AtomicBoolean resetLogging() {
    ILoggerFactory loggerFactory = LoggerFactory.getILoggerFactory();
    final AtomicBoolean logMessageReceived = new AtomicBoolean(false);

    if (loggerFactory instanceof LoggerContext) {
      LoggerContext loggerContext = (LoggerContext) loggerFactory;

      AppenderBase<ILoggingEvent> appender = new AppenderBase<ILoggingEvent>() {
        @Override
        protected void append(ILoggingEvent eventObject) {
          logMessageReceived.set(true);
        }
      };
      loggerContext.getLogger(DataMigration.class).addAppender(appender);
      appender.setContext(loggerContext);
      appender.start();
    }
    return logMessageReceived;
  }
}
