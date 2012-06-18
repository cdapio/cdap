package com.continuuity.common.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
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
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CConfiguration extends Configuration {
  @SuppressWarnings("unused")
  private static final Logger LOG =
      LoggerFactory.getLogger(CConfiguration.class);

  /**
   * Adds Continuuity related configuration files to the Configuration object.
   *
   * @param conf  The configuration object to add the resources to.
   * @return Configuration object.
   */
  public static CConfiguration addResources(CConfiguration conf) {

    // Clear any default properties
    conf.clear();
    return conf;
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
    return addResources(conf);
  }

} // end of CConfiguration class
