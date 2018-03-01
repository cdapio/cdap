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

package co.cask.cdap.examples.report;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Aggregator;

/**
 *
 */
public class ProgramRunMetaAggregator extends Aggregator<Row, ReportRecordBuilder, ReportRecordBuilder> {

  @Override
  public ReportRecordBuilder zero() {
    return new ReportRecordBuilder();
  }

  @Override
  public ReportRecordBuilder reduce(ReportRecordBuilder recordBuilder, Row row) {
    recordBuilder.setProgramRunStatus("program", "run", "STARTING", 1000L, "user");
    return recordBuilder;
  }

  @Override
  public ReportRecordBuilder merge(ReportRecordBuilder b1, ReportRecordBuilder b2) {
    return b1;
  }

  @Override
  public ReportRecordBuilder finish(ReportRecordBuilder reduction) {
    return reduction;
  }
}
