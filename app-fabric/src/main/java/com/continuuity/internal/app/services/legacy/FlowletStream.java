package com.continuuity.internal.app.services.legacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

public class FlowletStream {

  private static final Logger Log = LoggerFactory.getLogger(FlowStream.class);

  /**
   * Build the default URI for an out stream of a flowlet,
   * which is queue://flowname/flowletname/streamname
   * @param flowName the name of the flow
   * @param flowletName the name of the flowlet
   * @param streamName the name of the flowlet output stream
   * @return the URI for the stream
   */
  static public URI defaultURI(String flowName, String flowletName, String streamName) {
    try {
      return new URI("queue", flowName, "/" + flowletName + "/" + streamName, null);
    } catch (URISyntaxException e) {
      Log.error("Cannot construct a valid URI from flow id {}, flowlet name {} and stream name {}: {}",
          new Object[] { flowName, flowletName, streamName, e.getMessage() });
      throw new IllegalArgumentException("Invalid flow id '"
          + flowName + "', flowlet name '" + flowletName + "' and stream name '" + streamName + "'.", e);
    }
  }
}
