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

package io.cdap.cdap.common.conf;

import java.io.File;
import java.net.MalformedURLException;

/**
 * Used to create Configuration Object for security related properties
 */
public class SConfiguration extends Configuration {

  private SConfiguration() {
    // Shouldn't be used other than in this class.
  }

  private SConfiguration(SConfiguration other) {
    super(other);
  }

  /**
   * Creates an instance of {@link SConfiguration}.
   *
   * @return an instance of SConfiguration.
   */
  public static SConfiguration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    SConfiguration conf = new SConfiguration();
    conf.addResource("cdap-security.xml");
    return conf;
  }

  /**
   * Creates an instance of configuration.
   * @param file the file to be added to the configuration
   * @return an instance of SConfiguration
   * @throws java.net.MalformedURLException if the error occurred while constructing the URL
   */
  public static SConfiguration create(File file) throws MalformedURLException {
    SConfiguration conf = new SConfiguration();
    conf.addResource(file.toURI().toURL());
    return conf;
  }

  /**
   * Creates a new instance which clones all configurations from another {@link SConfiguration}.
   */
  public static SConfiguration copy(SConfiguration other) {
    return new SConfiguration(other);
  }
}
