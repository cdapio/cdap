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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.logging.LoggingUtil;
import co.cask.cdap.logging.framework.InvalidPipelineException;
import co.cask.cdap.logging.framework.LogPipelineLoader;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Checks for log appender configurations. This class will be automatically picked up by the MasterStartupTool.
 */
@SuppressWarnings("unused")
class LogPipelineCheck extends AbstractMasterCheck {

  private static final Logger LOG = LoggerFactory.getLogger(LogPipelineCheck.class);

  @Inject
  LogPipelineCheck(CConfiguration cConf) {
    super(cConf);
  }

  @Override
  public void run() throws Exception {
    // Because the logging framework supports usage of addition jars to host appender classes,
    // for validations, we need to construct a new classloader using all system jars plus the configured
    // additional log library jars.
    // A new classloader is needed because the logback doesn't support context classloader and requires the
    // LoggerContext class needs to be loaded from the same classloader as all appender classes.
    // We need to use reflection to load the LogPipelineLoader class and call validate on it

    // Collects all URLS used by the CDAP system classloader
    List<URL> urls = ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new ArrayList<URL>());
    for (File libJar : LoggingUtil.getExtensionJars(cConf)) {
      urls.add(libJar.toURI().toURL());
    }

    // Serialize the cConf to a String. This is needed because the cConf field is
    // loaded from the CDAP system classloader and cannot be passed directly to the
    // LogPipelineLoader class that loaded from the new ClassLoader constructed above.
    StringWriter writer = new StringWriter();
    this.cConf.writeXml(writer);

    // Create a new classloader and run the following code using reflection
    //
    // CConfiguration cConf = CConfiguration.create();
    // cConf.clear();
    // cConf.addResource(inputStream);
    // new LogPipelineLoader(cConf).validate();
    ClassLoader classLoader = new URLClassLoader(urls.toArray(new URL[urls.size()]), null);

    Class<?> cConfClass = classLoader.loadClass(CConfiguration.class.getName());
    Object cConf = cConfClass.getMethod("create").invoke(null);
    cConfClass.getMethod("clear").invoke(cConf);

    InputStream input = new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8));
    cConfClass.getMethod("addResource", InputStream.class).invoke(cConf, input);

    Class<?> loaderClass = classLoader.loadClass(LogPipelineLoader.class.getName());
    Object loader = loaderClass.getConstructor(cConfClass).newInstance(cConf);

    try {
      loaderClass.getMethod("validate").invoke(loader);
    } catch (InvocationTargetException e) {
      // Translate the exception throw by the reflection call
      Throwable cause = e.getCause();
      // Because the "InvalidPipelineException" throw from the reflection call is actually loaded by a different
      // classloader, we need to recreate a new one with the current classloader using the original cause
      // and stacktrace to allow easy debugging.
      // From the perspective of the caller to this method, it doesn't see any classloader trick being done in here.
      // Ideally we should do it for any cause class that is loaded from the classloader constructed above, but
      // that would make the code unnecessarily complicate since we know that only InvalidPipelineException
      // will be throw from the validate method.
      if (InvalidPipelineException.class.getName().equals(cause.getClass().getName())) {
        InvalidPipelineException ex = new InvalidPipelineException(cause.getMessage(), cause.getCause());
        ex.setStackTrace(cause.getStackTrace());
        throw ex;
      }
      Throwables.propagateIfPossible(cause, Exception.class);
      throw new RuntimeException(cause);
    }

    LOG.info("Log pipeline configurations verified.");
  }
}
