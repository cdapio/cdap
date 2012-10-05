package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;

/**
 * This is the base class for all the gateway's modules. Regardless of the type
 * of the connector, each connector must have a name, a method to configure it,
 * and a method to start and stop it.
 * <p/>
 * Upon start-up, the gateway loads, initializes and starts each connector as
 * follows:
 * <ol>
 * <li>Instantiate the connector using the default constructor.</li>
 * <li>Set the name of the connector via connector.setName().</li>
 * <li>Configure the connector by calling connector.configure().</li>
 * <li>Connector-type specific initialization (such as, setConsumer() for a
 *    collector).</li>
 * <li>Start the connector via connector.start()</li>
 * <li>...</li>
 * <li>Stop the connector via connector.stop()</li>
 * </ol>
 */
public abstract class Connector {

  /**
   * The name of this connector, it must be unique
   */
  protected String name;

  /**
   * This is our configuration
   */
  protected CConfiguration myConfiguration;

  /**
   * This will be used to collect connector metrics
   */
  protected CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * Retrieve the metrics client of the connector
   */
  public CMetrics getMetricsClient() {
    return this.metrics;
  }

  /**
   * Configure this connector
   *
   * @param configuration The configuration
   */

  public void configure(CConfiguration configuration) throws Exception {
    this.myConfiguration = configuration;
  }

  /**
   * Get this connector's configuration
   *
   * @return the configuration
   */
  public CConfiguration getConfiguration() {
    return this.myConfiguration;
  }

  /**
   * Set the name of this connector.
   *
   * @param name The name to be set. It must be unique.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Get the name of this connector
   *
   * @return the name of the connector
   */
  public String getName() {
    return this.name;
  }

  /**
   * Start this connector. After this, the connector is assumed to be fully
   * operational.
   *
   * @throws Exception if any exception occurs during start up
   */
  public abstract void start() throws Exception;

  /**
   * Stop this connector. This should perform all necessary deinitialization,
   * such as closing files or sockets.
   *
   * @throws Exception if any exception occurs during stop
   */
  public abstract void stop() throws Exception;
}
