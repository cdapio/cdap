/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ConfigurationJsonToolTest {

  @Test
  public void testDeprecatedKeys() {
    // Add a log appender to intercept logs from the Configuration object
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
      loggerContext.getLogger(Configuration.class).addAppender(appender);
      appender.setContext(loggerContext);
      appender.start();
    }

    // Dump the configuration as json
    StringBuilder builder = new StringBuilder();
    ConfigurationJsonTool.exportToJson(SConfiguration.create(), builder);
    String address = new Gson().fromJson(builder.toString(), JsonObject.class)
                               .get("security.auth.server.bind.address").getAsString();
    Assert.assertEquals("0.0.0.0", address);

    // There shouldn't be any log message, even we access the deprecated key
    Assert.assertFalse(logMessageReceived.get());

    // If we access the deprecated key directly, there should still be deprecated key warning.
    // That's because the log level shouldn't been reset.
    SConfiguration.create().get("security.auth.server.bind.address");
    Assert.assertTrue(logMessageReceived.get());
  }
}
