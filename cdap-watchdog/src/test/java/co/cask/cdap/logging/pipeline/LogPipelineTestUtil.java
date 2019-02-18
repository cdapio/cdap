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

package co.cask.cdap.logging.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.appender.ForwardingAppender;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * Util class for log pipeline unit test.
 */
public final class LogPipelineTestUtil {
  /**
   * Returns appender from provided logger, name and class.
   */
  public static <T extends Appender<ILoggingEvent>> T getAppender(Logger logger, String name, Class<T> cls) {
    Appender<ILoggingEvent> appender = logger.getAppender(name);
    while (!cls.isAssignableFrom(appender.getClass())) {
      if (appender instanceof ForwardingAppender) {
        appender = ((ForwardingAppender<ILoggingEvent>) appender).getDelegate();
      } else {
        throw new RuntimeException("Failed to find appender " + name + " of type " + cls.getName());
      }
    }
    return cls.cast(appender);
  }

  /**
   * Creates a new {@link ILoggingEvent} with the given information.
   */
  public static ILoggingEvent createLoggingEvent(String loggerName, Level level, String message, long timestamp) {
    LoggingEvent event = new LoggingEvent();
    event.setLevel(level);
    event.setLoggerName(loggerName);
    event.setMessage(message);
    event.setTimeStamp(timestamp);
    return event;
  }

  /**
   * Creates logger context.
   */
  public static LoggerContext createLoggerContext(String rootLevel,
                                                  Map<String, String> loggerLevels,
                                                  String appenderClassName) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element configuration = doc.createElement("configuration");
    doc.appendChild(configuration);

    Element appender = doc.createElement("appender");
    appender.setAttribute("name", "Test");
    appender.setAttribute("class", appenderClassName);
    configuration.appendChild(appender);

    for (Map.Entry<String, String> entry : loggerLevels.entrySet()) {
      Element logger = doc.createElement("logger");
      logger.setAttribute("name", entry.getKey());
      logger.setAttribute("level", entry.getValue());
      configuration.appendChild(logger);
    }

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
    JoranConfigurator configurator = new LogPipelineConfigurator(CConfiguration.create());
    configurator.setContext(context);
    configurator.doConfigure(new InputSource(new StringReader(writer.toString())));

    return context;
  }

  private LogPipelineTestUtil() {
    // no-op
  }
}
