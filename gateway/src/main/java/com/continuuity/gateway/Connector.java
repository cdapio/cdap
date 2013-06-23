package com.continuuity.gateway;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.metrics.CMetrics;
import com.continuuity.common.metrics.MetricType;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.metadata.MetadataService;
import com.continuuity.weave.discovery.DiscoveryServiceClient;

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
 * collector).</li>
 * <li>Start the connector via connector.start()</li>
 * <li>...</li>
 * <li>Stop the connector via connector.stop()</li>
 * </ol>
 */
public abstract class Connector {

  /**
   * The name of this connector, it must be unique.
   */
  private String name;

  /**
   * The metrics qualifier to be used for this connector.
   */
  private String metricsQualifier;

  /**
   * This is our configuration.
   */
  private CConfiguration myConfiguration;

  /**
   * This will be used to collect connector metrics.
   */
  private CMetrics metrics = new CMetrics(MetricType.System);

  /**
   * This is for discovery service.
   */
  private DiscoveryServiceClient discoveryServiceClient;

  /**
   * Authenticates requests to this connector.
   */
  private GatewayAuthenticator authenticator;

  /**
   * The meta data service.
   */
  private MetadataService mds;

  /**
   * Retrieve the metrics client of the connector.
   */
  public CMetrics getMetricsClient() {
    return this.metrics;
  }

  /**
   * Configure this connector.
   *
   * @param configuration The configuration
   */

  public void configure(CConfiguration configuration) throws Exception {
    this.myConfiguration = configuration;
  }

  /**
   * Get this connector's configuration.
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
    this.metricsQualifier = Constants.GATEWAY_PREFIX + name;
  }

  /**
   * Get the name of this connector.
   *
   * @return the name of the connector
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get the metrics qualifier for this connector (gateway.connector.name).
   *
   * @return the metrics qualifier
   */
  public String getMetricsQualifier() {
    return this.metricsQualifier;
  }

  void setDiscoveryServiceClient(DiscoveryServiceClient client) {
    discoveryServiceClient = client;
  }

  public DiscoveryServiceClient getDiscoveryServiceClient() {
    return discoveryServiceClient;
  }

  /**
   * Sets the authenticator to be used for all requests to this connector.
   *
   * @param authenticator the authenticator to use for requests
   */
  public void setAuthenticator(GatewayAuthenticator authenticator) {
    this.authenticator = authenticator;
  }

  /**
   * Returns the authenticator to be used for all requests to this connector.
   *
   * @return authenticator to use for requests
   */
  public GatewayAuthenticator getAuthenticator() {
    return this.authenticator;
  }

  /**
   * Set the meta data service for this collector.
   *
   * @param service the metadata servrice to use
   */
  public void setMetadataService(MetadataService service) {
    this.mds = service;
  }

  public MetadataService getMetadataService() {
    return this.mds;
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

  private GatewayMetrics gatewayMetrics;

  public void setGatewayMetrics(GatewayMetrics metrics) {
    this.gatewayMetrics = metrics;
  }

  public GatewayMetrics getGatewayMetrics() {
    return gatewayMetrics;
  }
}
