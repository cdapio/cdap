package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/**
 * Helper to generate Gateway Url.
 */
public class GatewayUrlGenerator {

  /**
   * Creates gateway base url by using given hostname and port. If they are null, then tries to read
   * the gateway config to get hostname and port.
   * @return The base url if found, or null otherwise.
   */
  public static String getBaseUrl(CConfiguration config, String hostname, int port, boolean ssl) {

    if (port <= 0) {
      for (String rule : config.getStrings(Constants.Router.FORWARD, Constants.Router.DEFAULT_FORWARD)) {
        Iterable<String> portService = Splitter.on(':').split(rule);
        if (Iterables.get(portService, 1).equals(Constants.Service.GATEWAY)) {
          try {
            port = Integer.parseInt(Iterables.get(portService, 0));
          } catch (NumberFormatException e) {
            // This is called from a command line client. Logging is turned off, ignore the error
          }
          break;
        }
      }
    }

    if (hostname == null) {
      hostname = config.get(Constants.Router.ADDRESS);
    }

    if (port <= 0 || hostname == null) {
      return null;
    }

    return (ssl ? "https" : "http") + "://"
      + hostname + ":"
      + port
      + "/v2/";
  }
}
