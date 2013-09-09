package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.v2.GatewayConstants;

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
      port = config.getInt(GatewayConstants.ConfigKeys.PORT, -1);
    }

    if (hostname == null) {
      hostname = config.get(GatewayConstants.ConfigKeys.ADDRESS);
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
