package com.continuuity.common.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CConfiguration is an extension of the Hadoop Configuration class. By default,
 * this class attempts to look for configuration options in the following files:
 * TODO: Do we really need two files, named like this?
 * * <ul>
 *   <li>continuuity-default.xml</li>
 *   <li>continuuity-site.xml</li>
 * </ul>
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
  public static Configuration addResources(Configuration conf) {

    // Clear any default properties
    conf.clear();

    // Load our Continuuity resources
    conf.addResource("continuuity-default.xml");
    conf.addResource("continuuity-site.xml");

    return conf;
  }

  /**
   * Creates an instance of configuration.
   *
   * @return an instance of CConfiguration.
   */
  public static Configuration create() {

    // Create a new configuration instance, but do NOT initialize with
    // the Hadoop default properties.
    Configuration conf = new Configuration(false);

    return addResources(conf);
  }

  /**
   * Clone a Configuration and create a new instance from existing values.
   *
   * @param that  Other configuration.
   * @return A cloned Configuration instance.
   */
  public static Configuration clone(final Configuration that) {
    return new Configuration(that);
  }

} // end of CConfiguration class
