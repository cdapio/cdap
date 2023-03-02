/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.http;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility functions for processing HTTP response codes.
 * TODO(CDAP-20266): Move this code to a common module avoid code duplication.
 */
public final class HttpCodes {

  private HttpCodes() {
  }

  /**
   * List of HTTP error codes that are retryable if the request is idempotent. NOTE:
   * HTTP_UNAVAILABLE also retryable for non-idempotent requests.
   */
  private static final List<Integer> HTTP_SERVER_ERROR_CODES = Collections.unmodifiableList(
      Arrays.asList(HttpURLConnection.HTTP_BAD_GATEWAY,
          HttpURLConnection.HTTP_GATEWAY_TIMEOUT,
          HttpURLConnection.HTTP_INTERNAL_ERROR,
          HttpURLConnection.HTTP_UNAVAILABLE));

  /**
   * Returns {@code true} if the idempotent request is retryable
   *
   * @param responseCode response code returned from executing the request.
   */
  public static boolean isRetryable(int responseCode) {
    return HTTP_SERVER_ERROR_CODES.contains(responseCode);
  }
}
