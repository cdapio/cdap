/*
 * Copyright © 2019 Cask Data, Inc.
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
 * Represents a batch query result of {@link ApplicationDetail}.
 */
public class BatchApplicationDetail {

  private final int statusCode;
  private final ApplicationDetail detail;
  private final String error;

  public BatchApplicationDetail(ApplicationDetail detail) {
    this.statusCode = 200;
    this.detail = detail;
    this.error = null;
  }

  public BatchApplicationDetail(HttpErrorStatusProvider statusProvider) {
    this.statusCode = statusProvider.getStatusCode();
    this.error = statusProvider.getMessage();
    this.detail = null;
  }

  /**
   * Returns the HTTP status of the response.
   */
  public int getStatusCode() {
    return statusCode;
  }

  /**
   * Returns the {@link ApplicationDetail} or {@code null} if the status code is non-200.
   */
  @Nullable
  public ApplicationDetail getDetail() {
    return detail;
  }

  /**
   * Returns the error string if the status code is non-200; otherwise return {@code null}.
   */
  @Nullable
  public String getError() {
    return error;
  }
}
