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

class RecordAgg extends org.apache.spark.sql.expressions.Aggregator[org.apache.spark.sql.Row, RecordBuilder, Record] {

  def zero: RecordBuilder = RecordBuilder("", "", "", Vector.empty, None)
  def reduce(b: RecordBuilder, a: org.apache.spark.sql.Row): RecordBuilder = {
    val startInfo = if (b.startInfo.isDefined) b.startInfo else {
      val startInfoRow = Option(a.getAs[org.apache.spark.sql.Row](Constants.START_INFO))
      startInfoRow match {
        case None => None
        case Some(v) => Some(StartInfo(v.getAs(Constants.USER),
          v.getAs(Constants.RUNTIME_ARGUMENTS)))
      }
    }
    RecordBuilder(a.getAs(Constants.NAMESPACE), a.getAs(Constants.PROGRAM),
      a.getAs(Constants.RUN), b.statuses :+ (a.getAs[String](Constants.STATUS), a.getAs[Long](Constants.TIME)),
      startInfo)
  }
  def merge(b1: RecordBuilder, b2: RecordBuilder) = {
    b1.merge(b2)
  }
  def finish(b: RecordBuilder): Record = {
    b.build()
  }
  def bufferEncoder()= org.apache.spark.sql.Encoders.product[RecordBuilder]
  def outputEncoder()= org.apache.spark.sql.Encoders.product[Record]
}