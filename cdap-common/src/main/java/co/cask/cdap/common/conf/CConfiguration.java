/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

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

  private CConfiguration() {
    // Shouldn't be used other than in this class.
  }

  private CConfiguration(Configuration other) {
    super(other);
  }

  /**
   * Creates an instance of configuration.
   *
   * @return an instance of CConfiguration.
   */
  public static CConfiguration create() {
    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    CConfiguration conf = new CConfiguration();
    conf.addResource("cdap-default.xml");
    conf.addResource("cdap-site.xml");
    return conf;
  }

  /**
   * Creates an instance of configuration.
   * @param file the file to be added to the configuration
   * @param moreFiles the list of more files to be added to the configuration
   * @return an instance of CConfiguration
   * @throws MalformedURLException if the error occurred while constructing the URL
   */
  public static CConfiguration create(File file, File...moreFiles) throws MalformedURLException {
    CConfiguration conf = new CConfiguration();
    conf.addResource(file.toURI().toURL());
    for (File anotherFile : moreFiles) {
      conf.addResource(anotherFile.toURI().toURL());
    }
    return conf;
  }

  /**
   * Creates an instance of configuration.
   * @param resource the URL to be added to the configuration
   * @param moreResources the list of URL's to be added to the configuration
   * @return an instance of CConfiguration
   * @throws IllegalArgumentException when the resource cannot be converted to the URL
   */
  public static CConfiguration create(URL resource, URL...moreResources) {
    CConfiguration conf = new CConfiguration();
    conf.addResource(resource);
    for (URL resourceURL : moreResources) {
      conf.addResource(resourceURL);
    }
    return conf;
  }

  /**
   * Creates an instance of configuration.
   * @param resource the resource to be added to the configuration
   * @return an instance of CConfiguration
   */
  public static CConfiguration create(InputStream resource) {
    CConfiguration conf = new CConfiguration();
    conf.addResource(resource);
    return conf;
  }

  /**
   * Creates a new instance which clones all configurations from another {@link CConfiguration}.
   */
  public static CConfiguration copy(CConfiguration other) {
    return new CConfiguration(other);
  }
}
