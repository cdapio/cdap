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
 * Failure reasons reported by runtime token validation.
 */
enum RuntimeValidationFailure {

  LEGACY_TOKEN("legacy_token"),
  URL_MISMATCH("url_mismatch"),
  MISSING_SCOPES("missing_scopes"),
  API_NOT_ALLOWED("api_not_allowed"),
  SECRET("secret"),
  CONNECTION("connection"),
  OAUTH("oauth"),
  MESSAGE_LOGS("message_logs"),
  MESSAGE_METRICS("message_metrics"),
  MESSAGE_METADATA("message_metadata"),
  MESSAGE_AUDIT("message_audit"),
  MESSAGE_DATA_EVENT("message_data_event"),
  MESSAGE_TOPIC("message_topic");

  private final String reason;

  RuntimeValidationFailure(String reason) {
    this.reason = reason;
  }

  String getReason() {
    return reason;
  }

  String getMetricName() {
    return Constants.Metrics.RuntimeToken.VALIDATION_FAILURE_PREFIX + reason;
  }
}
