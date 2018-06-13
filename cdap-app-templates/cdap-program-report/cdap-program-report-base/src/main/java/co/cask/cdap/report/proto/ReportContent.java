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
 * Represents report content details in an HTTP response.
 */
public class ReportContent {
  private final long offset;
  private final int limit;
  private final long total;
  private final List<String> details;

  public ReportContent(long offset, int limit, long total, List<String> reports) {
    this.offset = offset;
    this.limit = limit;
    this.total = total;
    this.details = reports;
  }

  /**
   * @return the offset in the whole report from which the report records are added to this {@link ReportContent}
   */
  public long getOffset() {
    return offset;
  }

  /**
   * @return the max limit of number of report records contained in this {@link ReportContent}
   */
  public int getLimit() {
    return limit;
  }

  /**
   * @return the total number of report records in the whole report
   */
  public long getTotal() {
    return total;
  }

  /**
   * @return the records of program details in the report
   */
  public List<String> getDetails() {
    return details;
  }

  public String toJson() {
    return "{" +
      "\"offset\":" + offset +
      ", \"limit\":" + limit +
      ", \"total\":" + total +
      ", \"details\":" + details + // directly return details as JSON objects without stringifying them
      '}';
  }
}
