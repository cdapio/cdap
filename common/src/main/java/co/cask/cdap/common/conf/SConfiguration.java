package co.cask.cdap.common.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to create Configuration Object for security related properties
 */
public class SConfiguration extends Configuration {
  @SuppressWarnings("unused")
  private static final Logger LOG =
    LoggerFactory.getLogger(CConfiguration.class);

  private SConfiguration() {
    // Shouldn't be used other than in this class.
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

}
