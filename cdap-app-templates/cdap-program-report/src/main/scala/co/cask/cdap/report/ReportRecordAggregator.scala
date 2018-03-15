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

import java.lang.reflect.Type

import com.google.gson._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Aggregator

class ReportRecordAggregator extends org.apache.spark.sql.expressions.Aggregator[org.apache.spark.sql.Row, RecordBuilder, String] {

  import org.apache.spark.sql.Row

  def zero: RecordBuilder = RecordBuilder("", "", Vector.empty, None)
  def reduce(b: RecordBuilder, a: org.apache.spark.sql.Row): RecordBuilder = {
    val startInfo = if (b.startInfo.isDefined) b.startInfo else {
      val startInfoRow = Option(a.getAs[Row]("startInfo"))
//      println("startInfoRow = %s".format(startInfoRow))
      startInfoRow match {
        case None => None
        case Some(v) => Some(StartInfo(v.getAs("user"), v.getAs("runtimeArguments")))
      }
    }
//    println("startInfo = %s".format(startInfo))
    RecordBuilder(a.getAs("program"), a.getAs("run"), b.statuses :+ (a.getAs[String]("status"), a.getAs[Long]("time")), startInfo)
  }
  def merge(b1: RecordBuilder, b2: RecordBuilder) = {
    b1.merge(b2)
  }
  def finish(b: RecordBuilder): String = {
    ReportRecordAggregator.GSON.toJson(b.build())
  }
  def bufferEncoder()= org.apache.spark.sql.Encoders.product[RecordBuilder]
  def outputEncoder()= org.apache.spark.sql.Encoders.STRING
}


object ReportRecordAggregator {
  import com.google.gson._
  import java.lang.reflect.Type
  val optionSerializer = new com.google.gson.JsonSerializer[Option[Any]] {
    override def serialize(src: Option[Any], typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      src match {
        case None => JsonNull.INSTANCE
        case Some(v) => context.serialize(v)
      }
    }
  }
  val GSON = new GsonBuilder().registerTypeHierarchyAdapter(classOf[Option[Any]], optionSerializer).create()
}
