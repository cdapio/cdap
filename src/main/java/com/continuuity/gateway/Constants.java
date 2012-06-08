package com.continuuity.gateway;

/**
 * Constants is a utility class that contains a set of universal constants
 * that are used throughout the Gateway project.
 */
public class Constants {

  /**
   * The prefix for all continuity classes
   */
	static final String CONTINUUITY_PREFIX = "com.continuuity.";

  /**
   * The prefix for all gateway properties
   */
	static final String GATEWAY_PREFIX = "gateway.";

	/**
   * Used by the external client to identify and authenticate the client
   */
	public static final String HEADER_CLIENT_TOKEN
      = CONTINUUITY_PREFIX + "token";

	/**
   * Used by the external client to indicate what end point an event goes to
   */
	public static final String HEADER_DESTINATION_STREAM
      = CONTINUUITY_PREFIX + "destination";

	/**
   * Created by gateway to annotate each event with the name of the collector
   * through which it was ingested
   */
	public static final String HEADER_FROM_COLLECTOR
      = CONTINUUITY_PREFIX + "collector";

	/**
   * List of named collectors for the gateway
   */
	public static final String CONFIG_CONNECTORS
      = GATEWAY_PREFIX + "connectors";

  /**
   * The string used to identify configuration information
   */
  public static final String CONFIG_CONFIG = "config";

	/**
   * Class name of a named collector
   */
	public static final String CONFIG_CLASSNAME = "class";

	/**
   * Port number of a collector
   */
	public static final String CONFIG_PORT = "port";

	/**
	 * Whether an HTTP collector supports chunked requests
	 */
	public static final String CONFIG_CHUNKING = "chunk";

	/**
	 * The maximal supported size of the content of an Http request
	 */
	public static final String CONFIG_MAX_SIZE= "maxsize";

	/**
   * Whether an HTTP collector supports SSL
   */
	public static final String CONFIG_SSL = "ssl" ;

	/**
   * Path prefix in an HTTP URL.
	 * For instance, in http://g.d.c/rest/stream/myStream the prefix is /rest
   */
	public static final String CONFIG_PATH_PREFIX = "prefix";

	/**
	 * Middle component in the HTTP URL
	 * For instance, in http://g.d.c/rest/stream/myStream the middle is /stream/
	 */
	public static final String CONFIG_PATH_MIDDLE = "middle";

  /**
   * Generate the name of a property option for a named Collector. Basically
   * a helper method that concats some strings.
   *
   * @param collectorName  The name of the Collector
   * @param propertyName   The name of the property
   *
   * @return  A fully composed collector configuration property
   */
  public static String buildConnectorPropertyName(String collectorName,
																									String propertyName) {

    return collectorName + "." + propertyName;
  }

} // end of Constants class
