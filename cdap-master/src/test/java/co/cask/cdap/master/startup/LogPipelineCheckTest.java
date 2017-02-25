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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.framework.InvalidPipelineException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 *
 */
public class LogPipelineCheckTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testValid() throws Exception {
    File libDir = TEMP_FOLDER.newFolder();
    // Generate a jar containing the NoopAppender2.class, which we generate from NoopAppender by renaming it
    String appenderClassName = NoopAppender.class.getSimpleName() + "2";
    try (JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(new File(libDir, "lib.jar")))) {
      jarOutput.putNextEntry(new JarEntry(appenderClassName + ".class"));
      jarOutput.write(renameClass(NoopAppender.class, appenderClassName));
    }

    File configDir = TEMP_FOLDER.newFolder();
    generateLogConfig("ALL", Collections.<String, String>emptyMap(),
                      appenderClassName, new File(configDir, "test.xml"));

    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Logging.PIPELINE_LIBRARY_DIR, libDir.getAbsolutePath());
    cConf.set(Constants.Logging.PIPELINE_CONFIG_DIR, configDir.getAbsolutePath());

    new LogPipelineCheck(cConf).run();
  }

  @Test (expected = InvalidPipelineException.class)
  public void testInvalid() throws Exception {
    // Sets an invalid config for the CDAPLogAppender
    CConfiguration cConf = CConfiguration.create();
    cConf.set("log.pipeline.cdap.file.sync.interval.bytes", "-1");

    new LogPipelineCheck(cConf).run();
  }

  /**
   * Creates a logback configuration file at the given location.
   */
  private File generateLogConfig(String rootLevel, Map<String, String> loggerLevels,
                                 String appenderClassName, File file) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element configuration = doc.createElement("configuration");
    doc.appendChild(configuration);

    int idx = appenderClassName.lastIndexOf('.');
    String appenderName = idx >= 0 ? appenderClassName.substring(idx + 1) : appenderClassName;

    Element appender = doc.createElement("appender");
    appender.setAttribute("name", appenderName);
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
    appenderRef.setAttribute("ref", appenderName);
    rootLogger.appendChild(appenderRef);

    configuration.appendChild(rootLogger);

    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      transformer.transform(new DOMSource(doc), new StreamResult(writer));
    }
    return file;
  }

  /**
   * Generates the bytecode of a new class with the given name using the given class as the skeleton.
   */
  private byte[] renameClass(final Class<?> cls, final String newClassName) throws IOException {
    ClassReader cr = new ClassReader(cls.getName());
    ClassWriter cw = new ClassWriter(0);
    final String oldTypeName = Type.getInternalName(cls);
    cr.accept(new RemappingClassAdapter(cw, new Remapper() {
      @Override
      public String mapType(String type) {
        return oldTypeName.equals(type) ? newClassName : type;
      }
    }), 0);
    return cw.toByteArray();
  }
}
