/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import io.cdap.cdap.common.conf.Constants;

/**
 * Non-failure scope admission categories tracked when a callback is admitted through a dynamic flag
 * or wildcard scope entry.
 */
enum RuntimeScopeAdmission {

  DYNAMIC_SECRET("dynamic_secret", Constants.Metrics.RuntimeToken.DYNAMIC_SECRET_READ),
  WILDCARD_CONNECTION("wildcard_connection",
      Constants.Metrics.RuntimeToken.WILDCARD_CONNECTION_READ),
  WILDCARD_OAUTH("wildcard_oauth", Constants.Metrics.RuntimeToken.WILDCARD_OAUTH_READ);

  private final String reason;
  private final String metricName;

  RuntimeScopeAdmission(String reason, String metricName) {
    this.reason = reason;
    this.metricName = metricName;
  }

  String getReason() {
    return reason;
  }

  String getMetricName() {
    return metricName;
  }
}
