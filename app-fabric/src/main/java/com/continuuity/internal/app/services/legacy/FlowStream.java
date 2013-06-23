package com.continuuity.internal.app.services.legacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public final class FlowStream {

  private static final Logger Log = LoggerFactory.getLogger(FlowStream.class);

	/**
	 * Build the default URI for a stream, which is:
   *   stream://accountname/streamname
	 * @param accountName the name of the account
	 * @param streamName the name of the stream
	 * @return the URI for the stream
	 */
	static public URI buildStreamURI(String accountName, String streamName) {
    try {
  		return new URI("stream", accountName, "/" + streamName, null);
    } catch (URISyntaxException e) {
      Log.error("Cannot construct a valid URI from account name {} and stream" +
          " name {}: {}",
          new Object[] { accountName, streamName, e.getMessage() });
      throw new IllegalArgumentException("Cannot construct URI from account " +
          "name "
          + accountName + " and stream name " + streamName + ".", e);
    }
	}
}
