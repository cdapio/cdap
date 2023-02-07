/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.proto.sourcecontrol;

import io.cdap.cdap.api.common.HttpErrorStatusProvider;
import io.cdap.cdap.proto.ApplicationDetail;

import javax.annotation.Nullable;

/**
 * Represents a batch push result of {@link ApplicationDetail}
 */
public class BatchApplicationPushResult {
  private final int statusCode;
  private final SourceControlMeta sourceControlMeta;
  private final String error;

  public BatchApplicationPushResult(SourceControlMeta sourceControlMeta) {
    this.statusCode = 200;
    this.sourceControlMeta = sourceControlMeta;
    this.error = null;
  }

  public BatchApplicationPushResult(HttpErrorStatusProvider statusProvider) {
    this.statusCode = statusProvider.getStatusCode();
    this.error = statusProvider.getMessage();
    this.sourceControlMeta = null;
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
   * Returns the fileHash of a pushed application or {@code null} if the status code is non-200.
   */
  @Nullable
  public SourceControlMeta getSourceControlMeta() {
    return sourceControlMeta;
  }
}
