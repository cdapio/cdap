package com.continuuity.gateway;

public class Constants {

	static final String prefix = "com.continuiity.";

	/** used by the external client ot identify and authenticate the client */
	public static final String HEADER_CLIENT_TOKEN         = prefix + "token";
	/** used by the external client ot indicate what end point an event goes to */
	public static final String HEADER_DESTINATION_ENDPOINT = prefix + "destination";
	/** created by gateway to annotate each event with the name of the connector through which it was ingested */
	public static final String HEADER_FROM_CONNECTOR       = prefix + "connector";
}
