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

package co.cask.cdap.common.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 *
 */
public class SamplingLoggerTest {

  @Test
  public void testSamplingLogger() throws Exception {
    LoggerContext loggerContext = createLoggerContext("ALL", TestAppender.class.getName());
    Logger logger = Loggers.sampling(loggerContext.getLogger("LoggerTest"), LogSamplers.onceEvery(10));

    for (int i = 0; i < 100; i++) {
      logger.info("Testing " + i);
    }

    Appender<ILoggingEvent> appender = loggerContext.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME)
      .getAppender("Test");

    Assert.assertTrue(appender instanceof TestAppender);

    List<ILoggingEvent> events = ((TestAppender) appender).getEvents();
    Assert.assertEquals(10, events.size());

    // Inspect the caller data, this class must be on top.
    for (ILoggingEvent event : events) {
      StackTraceElement[] callerData = event.getCallerData();
      Assert.assertTrue(callerData.length > 0);
      Assert.assertEquals(getClass().getName(), callerData[0].getClassName());
    }
  }

  private LoggerContext createLoggerContext(String rootLevel, String appenderClassName) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element configuration = doc.createElement("configuration");
    doc.appendChild(configuration);

    Element appender = doc.createElement("appender");
    appender.setAttribute("name", "Test");
    appender.setAttribute("class", appenderClassName);
    configuration.appendChild(appender);

    Element rootLogger = doc.createElement("root");
    rootLogger.setAttribute("level", rootLevel);
    Element appenderRef = doc.createElement("appender-ref");
    appenderRef.setAttribute("ref", "Test");
    rootLogger.appendChild(appenderRef);

    configuration.appendChild(rootLogger);

    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));

    LoggerContext context = new LoggerContext();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(new InputSource(new StringReader(writer.toString())));

    return context;
  }

  /**
   * A log appender used for testing. It just buffer all events passed to it.
   */
  public static final class TestAppender extends AppenderBase<ILoggingEvent> {

    private final List<ILoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(ILoggingEvent event) {
      // Capture the call stack so that we can inspect later.
      event.prepareForDeferredProcessing();
      event.getCallerData();
      events.add(event);
    }

    public List<ILoggingEvent> getEvents() {
      return events;
    }
  }
}
