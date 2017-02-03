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

package co.cask.cdap.logging.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
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
 * Unit-test for {@link EffectiveLevelProvider}.
 */
public class EffectiveLevelProviderTest {

  @Test
  public void testEffectiveLevel() throws Exception {
    LoggerContext context = new LoggerContext();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    configurator.doConfigure(new InputSource(new StringReader(generateLogback("WARN", ImmutableMap.of(
      "test", "INFO",
      "test.a", "ERROR",
      "test.a.X", "DEBUG",
      "test.a.X$1", "OFF"
    )))));

    EffectiveLevelProvider cache = new EffectiveLevelProvider(context, 10);

    Assert.assertEquals(Level.INFO, cache.getEffectiveLevel("test"));
    Assert.assertEquals(Level.ERROR, cache.getEffectiveLevel("test.a"));
    Assert.assertEquals(Level.DEBUG, cache.getEffectiveLevel("test.a.X"));
    Assert.assertEquals(Level.OFF, cache.getEffectiveLevel("test.a.X$1"));

    Assert.assertEquals(Level.WARN, cache.getEffectiveLevel("defaultToRoot"));
    Assert.assertEquals(Level.INFO, cache.getEffectiveLevel("test.defaultToTest"));
    Assert.assertEquals(Level.ERROR, cache.getEffectiveLevel("test.a.defaultToTestDotA"));
    Assert.assertEquals(Level.DEBUG, cache.getEffectiveLevel("test.a.X.defaultToTestDotADotX"));
  }

  private String generateLogback(String rootLevel, Map<String, String> loggerLevels) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element configuration = doc.createElement("configuration");
    doc.appendChild(configuration);

    for (Map.Entry<String, String> entry : loggerLevels.entrySet()) {
      Element logger = doc.createElement("logger");
      logger.setAttribute("name", entry.getKey());
      logger.setAttribute("level", entry.getValue());
      configuration.appendChild(logger);
    }

    Element rootLogger = doc.createElement("root");
    rootLogger.setAttribute("level", rootLevel);
    configuration.appendChild(rootLogger);

    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    StringWriter writer = new StringWriter();
    transformer.transform(new DOMSource(doc), new StreamResult(writer));

    return writer.toString();
  }
}
