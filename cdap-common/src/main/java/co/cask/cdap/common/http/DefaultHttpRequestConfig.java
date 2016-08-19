/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.http;

import co.cask.cdap.common.conf.Constants;
import co.cask.common.http.HttpRequestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;

/**
 * Class to uniformly configure CDAP HTTP requests with a user-configured timeout
 */
public class DefaultHttpRequestConfig extends HttpRequestConfig {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultHttpRequestConfig.class);
  private static final String SYSTEM_PROPERTY_PREFIX = "cdap.";

  public static final int DEFAULT_TIMEOUT = 60000;

  // System property names
  public static final String CONNECTION_TIMEOUT_PROPERTY_NAME =
    SYSTEM_PROPERTY_PREFIX + Constants.HTTP_CLIENT_CONNECTION_TIMEOUT_MS;
  public static final String  READ_TIMEOUT_PROPERTY_NAME =
    SYSTEM_PROPERTY_PREFIX + Constants.HTTP_CLIENT_READ_TIMEOUT_MS;

  public DefaultHttpRequestConfig() {
    super(getTimeoutFromSystemProperties(CONNECTION_TIMEOUT_PROPERTY_NAME),
          getTimeoutFromSystemProperties(READ_TIMEOUT_PROPERTY_NAME));
  }

  /**
   * @param verifySSLCert false, to disable certificate verifying in SSL connections. By default SSL certificate is
   *                      verified.
   */
  public DefaultHttpRequestConfig(boolean verifySSLCert) {
    super(getTimeoutFromSystemProperties(CONNECTION_TIMEOUT_PROPERTY_NAME),
          getTimeoutFromSystemProperties(READ_TIMEOUT_PROPERTY_NAME), verifySSLCert);
  }

  /**
   * @param verifySSLCert false, to disable certificate verifying in SSL connections. By default SSL certificate is
   *                      verified.
   * @param fixedLengthStreamingThreshold number of bytes in the request body to use fix length request mode. See
   *                                  {@link HttpURLConnection#setFixedLengthStreamingMode(int)}.
   */
  public DefaultHttpRequestConfig(boolean verifySSLCert, int fixedLengthStreamingThreshold) {
    super(getTimeoutFromSystemProperties(CONNECTION_TIMEOUT_PROPERTY_NAME),
          getTimeoutFromSystemProperties(READ_TIMEOUT_PROPERTY_NAME),
          verifySSLCert, fixedLengthStreamingThreshold);
  }

  private static int getTimeoutFromSystemProperties(String propertyName) {
    Integer value = Integer.getInteger(propertyName);
    if (value == null) {
      LOG.debug("Timeout property {} was not found in system properties.", propertyName);
      return DEFAULT_TIMEOUT;
    }
    return value;
  }
}
