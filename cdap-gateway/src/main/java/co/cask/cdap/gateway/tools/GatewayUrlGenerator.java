/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.tools;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;

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
      if (config.getBoolean(Constants.Security.SSL_ENABLED)) {
        port = Integer.parseInt(config.get(Constants.Router.ROUTER_SSL_PORT,
                                           Constants.Router.DEFAULT_ROUTER_SSL_PORT));
      } else {
        port = Integer.parseInt(config.get(Constants.Router.ROUTER_PORT, Constants.Router.DEFAULT_ROUTER_PORT));
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
