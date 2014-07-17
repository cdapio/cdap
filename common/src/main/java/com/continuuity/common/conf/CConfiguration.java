/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CConfiguration is an extension of the Hadoop Configuration class. By default,
 * this class provides a empty configuration. To add a set of resources from an
 * XML file, make sure the file is in the classpath and use
 * <code>config.addResource("my file name");</code>
 *
 * <strong>Note:</strong> This class will lazily load any configuration
 * properties, therefore you will not be able to access them until you
 * have called one of the getXXX methods at least once.
 */
public class CConfiguration extends Configuration {
  @SuppressWarnings("unused")
  private static final Logger LOG =
    LoggerFactory.getLogger(CConfiguration.class);

  /**
   * Creates an instance of configuration.
   *
   * @return an instance of CConfiguration.
   */
  public static CConfiguration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    CConfiguration conf = new CConfiguration();
    conf.addResource("continuuity-default.xml");
    conf.addResource("continuuity-site.xml");
    return conf;
  }

  public void copyTxProperties(org.apache.hadoop.conf.Configuration destination) {
    Properties props = getProps();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith("data.tx") || property.startsWith("tx.persist")) {
        destination.set(property, get(property));
      }
    }
  }
}
