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
public class ReportGenerationInfo {
  private final long created;
  private final ReportStatus status;
  private final ReportGenerationRequest request;
  private final ReportSummary summary;

  public ReportGenerationInfo(long created, ReportStatus status, ReportGenerationRequest request,
                              @Nullable ReportSummary summary) {
    this.created = created;
    this.status = status;
    this.request = request;
    this.summary = summary;
  }

  /**
   * @return the creation time of this report in seconds
   */
  public long getCreated() {
    return created;
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
