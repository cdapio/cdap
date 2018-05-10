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
 * Represents the meta information a report shared by {@link ReportStatusInfo} and {@link ReportGenerationInfo}
 */
public class ReportMetaInfo {
  private final String name;
  @Nullable
  private final String description;
  private final long created;
  @Nullable
  private final Long expiry;
  private final ReportStatus status;

  public ReportMetaInfo(String name, String description, long created, Long expiry, ReportStatus status) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.expiry = expiry;
    this.status = status;
  }

  /**
   * @return the name of the report
   */
  public String getName() {
    return name;
  }

  /**
   * @return the description of the report, or {@code null} if not set
   */
  @Nullable
  public String getDescription() {
    return description;
  }

  /**
   * @return the creation time of the report in seconds
   */
  public long getCreated() {
    return created;
  }

  /**
   * @return the expiry time of the report in seconds or {@code null} if the report is saved and will never expire
   */
  @Nullable
  public Long getExpiry() {
    return expiry;
  }

  /**
   * @return the report generation status
   */
  public ReportStatus getStatus() {
    return status;
  }
}
