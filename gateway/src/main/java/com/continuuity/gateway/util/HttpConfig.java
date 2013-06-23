package com.continuuity.gateway.util;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This configures options for an HTTP connector.
 * The general form of a server URL is
 * <code>http[s]://&lt;host>:&lt;port>/&lt;prefix/&lt;middle/&lt;resource/...
 * </code> For instance, the REST collector could listen on
 * <code>http://localhost:1066/rest/stream</code> as
 * its base URI, a valid POST to the stream named xyz would go to
 * <code>http://localhost:1066/rest/stream/xyz</code>. In this example the
 * port is 1066, the prefix <code>/rest</code>, the middle is
 * <code>/stream/</code>.
 */
public class HttpConfig {
  private static final Logger LOG = LoggerFactory.getLogger(HttpConfig.class);

  /**
   * default name is the name of the protocol.
   */
  public static final String DEFAULT_NAME = "http";
  /**
   * a default server host name.
   */
  public static final String DEFAULT_HOST = "localhost";
  /**
   * a default port for HTTP.
   */
  public static final int DEFAULT_PORT = 8080;
  /**
   * default prefix is just the root path.
   */
  public static final String DEFAULT_PREFIX = "";
  /**
   * default middle part is empty.
   */
  public static final String DEFAULT_MIDDLE = "/";
  /**
   * chunking is on by default.
   */
  public static final boolean DEFAULT_CHUNKING = true;
  /**
   * default max content size is 1MB.
   */
  public static final int DEFAULT_MAX_CONTENT_SIZE = 1024 * 1024;
  /**
   * default is no secure transport.
   */
  public static final boolean DEFAULT_SSL = false;
  /**
   * default number of worker threads.
   */
  public static final int DEFAULT_THREADS = Constants.DEFAULT_THREADS;

  /**
   * this is the name of the connector, needed to find the properties.
   */
  private String name = DEFAULT_NAME;
  /**
   * this is the hostname of the service.
   */
  private String host = DEFAULT_HOST;
  /**
   * this is the port of the service.
   */
  private int port = DEFAULT_PORT;
  /**
   * the path prefix (see above).
   */
  private String prefix = DEFAULT_PREFIX;
  /**
   * the path middle (see above).
   */
  private String middle = DEFAULT_MIDDLE;
  /**
   * the maximal size of content.
   */
  private int maxContentSize = DEFAULT_MAX_CONTENT_SIZE;
  /**
   * whether we should accept chunked requests.
   */
  private boolean chunk = DEFAULT_CHUNKING;
  /**
   * whether secure socket transport is on.
   */
  private boolean ssl = DEFAULT_SSL;
  /**
   * number of worker threads in the http server.
   */
  private int threads = DEFAULT_THREADS;

  /**
   * private because this would create a config without a name.
   */
  private HttpConfig() {
  }

  /**
   * Constructor takes the connector name.
   *
   * @param name The name of the connector
   */
  public HttpConfig(String name) {
    this.name = name;
  }

  /**
   * Return the name of the connector.
   *
   * @return the name
   */
  public String getName() {
    return this.name;
  }

  /**
   * Return the configured port.
   *
   * @return the port number
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Return the host name. This is mainly useful for clients trying to
   * figure out the address of the service.
   *
   * @return the host name
   */
  public String getHost() {
    return this.host;
  }

  /**
   * Return the configured path prefix.
   *
   * @return the path prefix
   */
  public String getPathPrefix() {
    return this.prefix;
  }

  /**
   * Return the middle component of the configured path.
   *
   * @return the path middle
   */
  public String getPathMiddle() {
    return this.middle;
  }

  /**
   * Is this service supporting HTTP chunks?
   *
   * @return whether chunking is supported
   */
  public boolean isChunking() {
    return this.chunk;
  }

  /**
   * Is this service using SSL?
   *
   * @return whether it is using SSL
   */
  public boolean isSsl() {
    return this.ssl;
  }

  public void setSsl(boolean ssl) {
    this.ssl = ssl;
  }

  /**
   * Return the maximal size of content supported.
   *
   * @return the maximal supported size
   */
  public int getMaxContentSize() {
    return this.maxContentSize;
  }

  /**
   * Return the number of worker threads configured for the server.
   *
   * @return the number of server threads
   */
  public int getThreads() {
    return this.threads;
  }

  /**
   * Set the port of the service.
   *
   * @param port The port number
   * @return this HttpConfig object
   */
  public HttpConfig setPort(int port) {
    this.port = port;
    return this;
  }

  /**
   * Set the path prefix.
   *
   * @param prefix The path prefix to use
   * @return this HttpConfig object
   */
  public HttpConfig setPathPrefix(String prefix) {
    if (prefix == null) {
      throw new IllegalArgumentException("Path prefix may not be null.");
    }
    this.prefix = prefix;
    return this;
  }

  /**
   * Set the path middle.
   *
   * @param middle The path middle to use
   * @return this HttpConfig object
   */
  public HttpConfig setPathMiddle(String middle) {
    if (middle == null) {
      throw new IllegalArgumentException("Path middle may not be null.");
    }
    this.middle = middle;
    return this;
  }

  /**
   * Read this HTTP configuration from a CConfiguration and a set of defaults.
   *
   * @param name          The name of the connector
   * @param configuration The configuration that has all  the options
   * @param defaults      The defaults to use
   * @return a new HTTPConfig
   * @throws Exception if anything goes wrong
   */
  public static HttpConfig configure(String name,
                                     CConfiguration configuration,
                                     HttpConfig defaults) throws Exception {
    // if no defaults were given, create an empty config (it has defaults)
    if (defaults == null) {
      defaults = new HttpConfig();
    }
    HttpConfig config = new HttpConfig(name);
    config.host = configuration.get(Constants.CONFIG_HOSTNAME,
                                    defaults.getHost());
    config.port = configuration.getInt(Constants.buildConnectorPropertyName(
      name, Constants.CONFIG_PORT), defaults.getPort());
    config.threads = configuration.getInt(Constants.buildConnectorPropertyName(
      name, Constants.CONFIG_THREADS), defaults.getThreads());
    config.chunk = configuration.getBoolean(
      Constants.buildConnectorPropertyName(
        name, Constants.CONFIG_CHUNKING), defaults.isChunking());
    config.ssl = configuration.getBoolean(Constants.buildConnectorPropertyName(name,
                                                                               Constants.CONFIG_SSL), defaults.isSsl());

    //Set port to bind to 443
    if (config.ssl) {
      LOG.warn("SSL is not implemented yet. " +
                 "Ignoring configuration for connector '" + name + "'.");
      config.ssl = false;
    }

    config.prefix = configuration.get(Constants.buildConnectorPropertyName(
      name, Constants.CONFIG_PATH_PREFIX), defaults.getPathPrefix());
    config.middle = configuration.get(Constants.buildConnectorPropertyName(
      name, Constants.CONFIG_PATH_MIDDLE), defaults.getPathMiddle());
    config.maxContentSize = configuration.getInt(
      Constants.buildConnectorPropertyName(
        name, Constants.CONFIG_MAX_SIZE), defaults.getMaxContentSize());
    return config;
  }

  /**
   * Get the base URL that this HttpConfig describes.
   *
   * @param hostname the hostname to use for the base url (HttpConfig
   *                 does not have that). If null, localhost is used.
   * @return the base URL
   */
  public String getBaseUrl(String hostname) {
    return (this.isSsl() ? "https" : "http") + "://"
      + (hostname == null ? "localhost" : hostname) + ":"
      + this.getPort()
      + (this.getPathPrefix() == null ? "" : this.getPathPrefix())
      + (this.getPathMiddle() == null ? "" : this.getPathMiddle());
  }

  /**
   * Get the base URL that this HttpConfig describes, using localhost.
   *
   * @return the base URL
   */
  public String getBaseUrl() {
    return this.getBaseUrl(getHost());
  }
}
