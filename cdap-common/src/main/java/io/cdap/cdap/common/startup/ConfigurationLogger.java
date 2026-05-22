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
package io.cdap.cdap.common.startup;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs important configuration information.
 */
public class ConfigurationLogger {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationLogger.class);

  public static void logImportantConfig(CConfiguration cConf) {

    String classPath = System.getProperty("java.class.path");

    LOG.info("Master classpath: {}", classPath);

    LOG.info("Important config settings:");
    for (String featureToggleProp : Constants.FEATURE_TOGGLE_PROPS) {
      LOG.info("  {}: {}", featureToggleProp, cConf.get(featureToggleProp));
    }
    for (String portProp : Constants.PORT_PROPS) {
      LOG.info("  {}: {}", portProp, cConf.get(portProp));
    }
  }

}
