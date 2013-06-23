package com.continuuity.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

}
