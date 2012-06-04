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
	public static final String HEADER_DESTINATION_ENDPOINT
      = CONTINUUITY_PREFIX + "destination";

	/**
   * Created by gateway to annotate each event with the name of the connector
   * through which it was ingested
   */
	public static final String HEADER_FROM_CONNECTOR
      = CONTINUUITY_PREFIX + "connector";

	/**
   * List of named connectors for the gateway
   */
	public static final String CONFIG_CONNECTORS
      = GATEWAY_PREFIX + "connectors";

  /**
   * The string used to identify configuration information
   */
  public static final String CONFIG_CONFIG = "config";

	/**
   * Class name of a named connector
   */
	public static final String CONFIG_CLASSNAME = "class";

	/**
   * Port number of a connector
   */
	public static final String CONFIG_PORT = "port";

	/**
   * Whether an HTTP connector supports chunked requests
   */
	public static final String CONFIG_CHUNKING = "chunk";

	/**
   * Whether an HTTP connector supports SSL
   */
	public static final String CONFIG_SSL = "ssl" ;

	/**
   * Path prefix in an HTTP URL
   */
	public static final String CONFIG_PATH_PREFIX = "prefix";

	/**
   * Path component in the HTTP URL for a stream
   */
	public static final String CONFIG_PATH_STREAM = "stream";

  /**
   * Generate the name of a property option for the Gateway. Basically concats
   * the GATEWAY_PREFIX to the property.
   *
   * @param propertyName   The name of the property
   *
   * @return  A fully composed gateway configuration property
   */
  public static String buildGatewayPropertyName(String propertyName) {

    return GATEWAY_PREFIX + propertyName;
  }

  /**
   * Generate the name of a property option for a named Connector. Basically
   * a helper method that concats some strings.
   *
   * @param connectorName  The name of the Connector
   * @param propertyName   The name of the property
   *
   * @return  A fully composed connector configuration property
   */
  public static String buildConnectorPropertyName(String connectorName,
                                                  String propertyName) {

    return connectorName + "." + propertyName;
  }

} // end of Constants class
