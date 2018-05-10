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

/**
 * Represents the information of a report generation in an HTTP response.
 */
public class ReportGenerationInfo {
  private final String name;
  private final long created;
  private final ReportStatus status;
  private final ReportGenerationRequest request;

  public ReportGenerationInfo(String name, long created,
                              ReportStatus status, ReportGenerationRequest request) {
    this.name = name;
    this.created = created;
    this.status = status;
    this.request = request;
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
}
