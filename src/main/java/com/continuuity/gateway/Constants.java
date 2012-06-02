package com.continuuity.gateway;

public class Constants {

	static final String CONTINUUITY_PREFIX = "com.continuiity.";
	static final String GATEWAY_PREFIX = CONTINUUITY_PREFIX + "gateway.";

	/** used by the external client ot identify and authenticate the client */
	public static final String HEADER_CLIENT_TOKEN           = CONTINUUITY_PREFIX + "token";
	/** used by the external client ot indicate what end point an event goes to */
	public static final String HEADER_DESTINATION_ENDPOINT   = CONTINUUITY_PREFIX + "destination";
	/** created by gateway to annotate each event with the name of the connector through which it was ingested */
	public static final String HEADER_FROM_CONNECTOR         = CONTINUUITY_PREFIX + "connector";

	/** list of named connectors for the gateway */
	public static final String CONFIG_CONNECTORS             = GATEWAY_PREFIX + "connectors";

	/** compose the name of a config option for a named connector */
	public static String connectorConfigName(String connectorName, String configName) {
		return GATEWAY_PREFIX + connectorName + "." + configName;
	}

	/** class name of a named connector */
	public static final String CONFIG_CLASSNAME              = "class" ;
	/** port number of a connector */
	public static final String CONFIG_PORTNUMBER             = "port" ;
	/** whether an HTTP connector supports chunked requests */
	public static final String CONFIG_CHUNKING               = "chunk" ;
	/** whether an HTTTP connector supports SSL */
	public static final String CONFIG_SSL                    = "ssl" ;
	/** path prefix in an HTTP URL */
	public static final String CONFIG_PATH_PREFIX            = "prefix" ;
	/** path component in the HTTP URL for a stream */
	public static final String CONFIG_PATH_STREAM            = "stream" ;
}
