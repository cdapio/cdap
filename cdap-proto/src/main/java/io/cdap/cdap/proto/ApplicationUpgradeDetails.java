/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import javax.annotation.Nullable;

/**
 * Represents a application upgrade result of an {@link ApplicationDetail}.
 */
public class ApplicationUpgradeDetails {
  private final int statusCode;
  private final String error;
  private final String upgradeDetails;

  public ApplicationUpgradeDetails(String upgradeDetails, String error) {
    this.statusCode = 200;
    this.error = error;
    this.upgradeDetails = upgradeDetails;
  }

  public ApplicationUpgradeDetails(HttpErrorStatusProvider statusProvider) {
    this.statusCode = statusProvider.getStatusCode();
    this.error = statusProvider.getMessage();
    this.upgradeDetails = null;
  }

  /**
   * Returns the HTTP status of the response.
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Returns the error string if the status code is non-200; otherwise return {@code null}.
   */
  @Nullable
  public String getError() {
    return error;
  }

  /**
   * Returns the upgrade detail string if the status code is 200; otherwise return {@code null}.
   */
  @Nullable
  public String getUpgradeDetails() {
    return this.upgradeDetails;
  }

}
