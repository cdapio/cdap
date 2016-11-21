/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.api.log.LogProcessor;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ExtensionClassHelper;
import co.cask.cdap.common.lang.ExtensionClassLoader;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.InvalidExtensionException;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipException;

/**
 * class used to instantiate and get custom log processors
 */
public class CustomLogProcessors {
  private static final Logger LOG = LoggerFactory.getLogger(CustomLogProcessors.class);
  private static final String JAR_PATH_SEPARATOR = ",";

  private final CConfiguration cConf;
  private final InstantiatorFactory instantiatorFactory;
  private final String jarPath;

  public CustomLogProcessors(CConfiguration cConf) {
    this.cConf = cConf;
    this.instantiatorFactory = new InstantiatorFactory(false);
    this.jarPath = cConf.get(Constants.LogSaver.LOG_PROCESSOR_EXTENSION_JAR_PATH, "");
  }

  /**
   * Get the list of custom log processors
   * @return
   * @throws Exception
   */
  public List<LogProcessor> getLogProcessors() throws Exception {
    List<LogProcessor> logProcessors = new ArrayList<>();
    if (!jarPath.isEmpty()) {
      Iterator<String> jarPaths = Splitter.on(JAR_PATH_SEPARATOR).split(jarPath).iterator();
      while (jarPaths.hasNext()) {
        logProcessors.add(getLogProcessor(jarPaths.next()));
      }
    }
    return logProcessors;
  }

  private LogProcessor getLogProcessor(String logProcessorPluginJarPath) throws Exception {
    try {
      LOG.info("Loading Log Processor Jar file at {}", logProcessorPluginJarPath);
      File logProcessorExtenstionJar = new File(logProcessorPluginJarPath);
      ExtensionClassHelper.ensureValidExtensionJar(logProcessorExtenstionJar, "LogProcessor",
                                                   Constants.LogSaver.LOG_PROCESSOR_EXTENSION_JAR_PATH);
      File absoluteTmpFile = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                      cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
      File tmpDir = DirUtils.createTempDir(absoluteTmpFile);
      ExtensionClassLoader logProcessorClassLoader = createLogProcessorClassLoader(logProcessorExtenstionJar,
                                                                                      tmpDir);
      return createAndInitializeLogProcessor(logProcessorExtenstionJar, logProcessorClassLoader, tmpDir);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Get properties - properties that start with Constants.LogSaver.EXTENSION_CONFIG_PREFIX are obtained to populate
   * properties
   * @return
   */
  public Properties getLogProcessorExtensionProperties() {
    return ExtensionClassHelper.createExtensionProperties(cConf, Constants.LogSaver.EXTENSION_CONFIG_PREFIX);
  }

  private ExtensionClassLoader createLogProcessorClassLoader(File logProcessorJar, File tmpDir)
    throws IOException, InvalidExtensionException {
    LOG.info("Creating log processor extension using jar {}.", logProcessorJar);
    try {
      BundleJarUtil.unJar(Locations.toLocation(logProcessorJar), tmpDir);
      return new ExtensionClassLoader(tmpDir, CustomLogProcessors.class.getClassLoader(), LogProcessor.class);
    } catch (ZipException e) {
      throw new InvalidExtensionException(
        String.format("Log Processor extension jar %s specified as %s must be a jar file.", logProcessorJar,
                      Constants.LogSaver.LOG_PROCESSOR_EXTENSION_JAR_PATH), e
      );
    }
  }

  private LogProcessor createAndInitializeLogProcessor(File logProcessorJar,
                                                       ExtensionClassLoader logProcessorClassLoader,
                                                       File tmpDir) throws IOException, InvalidExtensionException {

    Class<? extends LogProcessor> logProcessorClass =
      ExtensionClassHelper.loadExtensionClass(logProcessorJar, logProcessorClassLoader,
                                              LogProcessor.class, tmpDir, "LogProcessor",
                                              LogProcessor.class.getName());
    // Set the context class loader to the LogProcessorClassLoader before creating a new instance of the extension,
    // so all classes required in this process are created from the LogProcessorClassLoader.
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(logProcessorClassLoader);
    LOG.debug("Setting context classloader to {}. Old classloader was {}.", logProcessorClassLoader, oldClassLoader);
    try {
      LogProcessor logProcessor;
      try {
        logProcessor = instantiatorFactory.get(TypeToken.of(logProcessorClass)).create();
      } catch (Exception e) {
        throw new InvalidExtensionException(
          String.format("Error while instantiating for log processor extension %s." +
                          "Please make sure that the extension " +
                          "is a public class with a default constructor.", logProcessorClass.getName()), e);
      }

      return logProcessor;
    } finally {
      // After the process of creation of a new instance has completed (success or failure), reset the context
      // classloader back to the original class loader.
      ClassLoaders.setContextClassLoader(oldClassLoader);
      LOG.debug("Resetting context classloader to {} from {}.", oldClassLoader, logProcessorClassLoader);
    }
  }

}
