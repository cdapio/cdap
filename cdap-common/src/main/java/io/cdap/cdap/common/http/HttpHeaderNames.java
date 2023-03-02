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

/**
 * Names for any Http headers introduced by CDAP should be placed here. This will help keeping the
 * context about headers in one place.
 */
public final class HttpHeaderNames {

  /**
   * Store request start time. Header should not be deleted or modified.
   */
  public static final String CDAP_REQ_TIMESTAMP_HDR = "CDAP_REQ_TIMESTAMP_HDR";

  // TODO move all other custom headers used by CDAP to this class. JIRA https://cdap.atlassian.net/browse/CDAP-18799

  // to prevent instantiation of this class
  private HttpHeaderNames() {
  }
}
