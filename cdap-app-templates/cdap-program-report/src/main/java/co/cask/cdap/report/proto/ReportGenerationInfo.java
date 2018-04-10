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

import javax.annotation.Nullable;

/**
 * Represents the information of a report generation in an HTTP response.
 */
public class ReportGenerationInfo {
  private final String name;
  @Nullable
  private final String description;
  private final long created;
  @Nullable
  private final Long expiry;
  private final ReportStatus status;
  @Nullable
  private final String error;
  private final ReportGenerationRequest request;
  @Nullable
  private final ReportSummary summary;

  public ReportGenerationInfo(String name, @Nullable String description, long created, @Nullable Long expiry,
                              ReportStatus status, @Nullable String error, ReportGenerationRequest request,
                              @Nullable ReportSummary summary) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.expiry = expiry;
    this.status = status;
    this.error = error;
    this.request = request;
    this.summary = summary;
  }


  /**
   * @return the name of the report
   */
  public String getName() {
    return name;
  }

  /**
   * @return the creation time of the report in seconds
   */
  public long getCreated() {
    return created;
  }

  /**
   * @return the description of the report, or {@code null} if not set
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return the expiry time of the report in seconds or {@code null} if the report is saved and will never expire
   */
  @Nullable
  public Long getExpiry() {
    return expiry;
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
   * @return the report generation status
   */
  public ReportStatus getStatus() {
    return status;
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
