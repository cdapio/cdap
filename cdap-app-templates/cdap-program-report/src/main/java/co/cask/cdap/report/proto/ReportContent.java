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

import java.util.List;

/**
 * Represents report records by a user in an HTTP response.
 */
public class ReportContent {
  private final long offset;
  private final int limit;
  private final long total;
  private final List<String> reports;

  public ReportContent(long offset, int limit, long total, List<String> reports) {
    this.offset = offset;
    this.limit = limit;
    this.total = total;
    this.reports = reports;
  }

  /**
   * @return the offset in the whole report from which the report records are added to this {@link ReportContent}
   */
  public long getOffset() {
    return offset;
  }

  /**
   * @return the max limit of number of report records
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return the actual total number of report records contained in this {@link ReportContent}
   */
  public long getTotal() {
    return total;
  }

  /**
   * @return the report records
   */
  public List<String> getReports() {
    return reports;
  }
}
