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

package co.cask.cdap.report;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
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
    if (row.getAs("status") == null) {
      return recordBuilder;
    }
    recordBuilder.setProgramRunStatus(row.getAs("program"), row.getAs("run"), row.getAs("status"),
                                      row.getAs("time"), "user");
    return recordBuilder;
  }

  @Override
  public ReportRecordBuilder merge(ReportRecordBuilder b1, ReportRecordBuilder b2) {
    return b1.merge(b2);
  }

  @Override
  public ReportRecordBuilder finish(ReportRecordBuilder reduction) {
    return reduction;
  }

  @Override
  public Encoder<ReportRecordBuilder> bufferEncoder() {
    return (Encoder<ReportRecordBuilder>) Encoders.kryo(ReportRecordBuilder.class);
  }

  @Override
  public Encoder<ReportRecordBuilder> outputEncoder() {
    return (Encoder<ReportRecordBuilder>) Encoders.kryo(ReportRecordBuilder.class);
  }
}
