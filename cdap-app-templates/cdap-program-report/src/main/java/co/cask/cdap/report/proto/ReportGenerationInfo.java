/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto;

import co.cask.cdap.report.proto.summary.ReportSummary;

import javax.annotation.Nullable;

/**
 * Represents the information of a report generation in an HTTP response.
 */
public class ReportGenerationInfo extends ReportMetaInfo {
  @Nullable
  private final String error;
  private final ReportGenerationRequest request;
  private final ReportSummary summary;

  public ReportGenerationInfo(String name, @Nullable String description, long created, @Nullable Long expiry,
                              ReportStatus status, @Nullable String error, ReportGenerationRequest request,
                              @Nullable ReportSummary summary) {
    super(name, description, created, expiry, status);
    this.error = error;
    this.request = request;
    this.summary = summary;
  }

  public ReportGenerationInfo(ReportMetaInfo metaInfo, @Nullable String error, ReportGenerationRequest request,
                              @Nullable ReportSummary summary) {
    this(metaInfo.getName(), metaInfo.getDescription(), metaInfo.getCreated(), metaInfo.getExpiry(),
         metaInfo.getStatus(), error, request, summary);
  }

  /**
   * @return the error of the report generation if the status of the report is {@link ReportStatus#FAILED},
   *         or {@code null} if the report status is not {@link ReportStatus#FAILED}
   */
  @Nullable
  public String getError() {
    return error;
  }

  /**
   * @return the request for generating this report
   */
  public ReportGenerationRequest getRequest() {
    return request;
  }

  /**
   * @return the summary of the report if the status of the report is {@link ReportStatus#COMPLETED},
   *         or {@code null} if the report status is not {@link ReportStatus#COMPLETED}
   */
  @Nullable
  public ReportSummary getSummary() {
    return summary;
  }
}
