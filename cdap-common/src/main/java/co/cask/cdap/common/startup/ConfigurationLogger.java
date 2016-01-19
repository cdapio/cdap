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
package co.cask.cdap.common.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Logs important configuration information.
 */
public class ConfigurationLogger {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationLogger.class);

  public static void logImportantConfig(CConfiguration cConf) {
    ClassLoader cl = ClassLoader.getSystemClassLoader();

    URL[] urls = ((URLClassLoader) cl).getURLs();

    StringBuilder classPath = new StringBuilder();
    LOG.info("Master classpath:");
    for (URL url : urls) {
      classPath.append(url.getFile()).append(":");
    }
    classPath.deleteCharAt(classPath.length() - 1);
    LOG.info(classPath.toString());

    LOG.info("Important config settings:");
    for (String featureToggleProp : Constants.FEATURE_TOGGLE_PROPS) {
      LOG.info("  {}: {}", featureToggleProp, cConf.get(featureToggleProp));
    }
    for (String portProp : Constants.PORT_PROPS) {
      LOG.info("  {}: {}", portProp, cConf.get(portProp));
    }
  }

}
