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
package co.cask.cdap.report

import co.cask.cdap.report.util.Constants
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator

/**
  * An aggregator that aggregates [[Row]]'s with the same program run ID into intermediate [[RecordBuilder]]'s
  * and finally merge the [[RecordBuilder]]'s to build a single [[Record]]
  */
class RecordAggregator extends Aggregator[Row, RecordBuilder, Record] {

  def zero: RecordBuilder = RecordBuilder("", "", "", Vector.empty, None)
  def reduce(builder: RecordBuilder, row: Row): RecordBuilder = {
    // Get the StartInfo from the builder if it exists or construct a new StartInfo from the row
    val startInfo = if (builder.startInfo.isDefined) builder.startInfo else {
      val startInfoRecord = Option(row.getAs[Row](Constants.START_INFO))
      startInfoRecord match {
        case None => None
        case Some(v) => Some(StartInfo(v.getAs(Constants.USER), v.getAs(Constants.RUNTIME_ARGUMENTS)))
      }
    }
    // Merge statusTimes from the builder with the new status and time tuple from the row. Combined with
    // the information from the row and the startInfo to create a new RecordBuilder
    RecordBuilder(row.getAs(Constants.NAMESPACE), row.getAs(Constants.PROGRAM), row.getAs(Constants.RUN),
      builder.statusTimes :+ (row.getAs[String](Constants.STATUS), row.getAs[Long](Constants.TIME)), startInfo)
  }
  def merge(b1: RecordBuilder, b2: RecordBuilder) = {
    b1.merge(b2)
  }
  def finish(b: RecordBuilder): Record = {
    b.build()
  }
  def bufferEncoder()= Encoders.product[RecordBuilder]
  def outputEncoder()= Encoders.product[Record]
}
