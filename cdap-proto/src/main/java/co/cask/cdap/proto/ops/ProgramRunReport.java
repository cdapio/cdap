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

package co.cask.cdap.proto.ops;

import java.util.List;

/**
 * Represents the program run report in an HTTP response.
 */
public class ProgramRunReport {
  private final long offset;
  private final long limit;
  private final long total;
  private final List<ProgramRunReportRecord> runs;

  public ProgramRunReport(long offset, long limit, long total, List<ProgramRunReportRecord> runs) {
    this.offset = offset;
    this.limit = limit;
    this.total = total;
    this.runs = runs;
  }

  public long getOffset() {
    return offset;
  }

  public long getLimit() {
    return limit;
  }

  public long getTotal() {
    return total;
  }

  public List<ProgramRunReportRecord> getRuns() {
    return runs;
  }
}
