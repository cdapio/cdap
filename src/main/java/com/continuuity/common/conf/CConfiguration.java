package com.continuuity.common.conf;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extension of Hadoop configuration to include Continuuity related
 * configurations.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CConfiguration extends Configuration {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(CConfiguration.class);

  /**
   * Adds continuuity related configuration files.
   * @param conf
   * @return Configuration object.
   */
  public static Configuration addResources(Configuration conf) {
    conf.clear();
    conf.addResource("continuuity-default.xml");
    conf.addResource("continuuity-site.xml");
    return conf;
  }

  /**
   * Creates an instance of configuration.
   * @return an instance of CConfiguration.
   */
  public static Configuration create() {
    Configuration conf = new Configuration();
    return addResources(conf);
  }

  /**
   * Creates an instance of configuration.
   * @param that Other configuration.
   * @return merged CConfiguration instance.
   */
  public static Configuration create(final Configuration that) {
    return new Configuration(that);
  }
}
