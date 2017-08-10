/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.spark.app

import co.cask.cdap.api.data.format.FormatSpecification
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.Schema
import co.cask.cdap.api.data.schema.Schema.Field
import co.cask.cdap.api.spark.AbstractSpark
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import co.cask.cdap.api.stream.GenericStreamEventData
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._

/**
  * Spark program for testing stream format specification usage.
  */
class ScalaStreamFormatSpecSpark extends AbstractSpark with SparkMain {

  case class Person(name: String, age: Int)

  override protected def configure() = {
    setMainClass(classOf[ScalaStreamFormatSpecSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext
    val sqlContext = new SQLContext(sc)

    // Read from CSV stream and turn it into a DataFrame
    val streamName = sec.getRuntimeArguments.get("stream.name")

    val fields = List(Field.of("name", Schema.of(Schema.Type.STRING)), Field.of("age", Schema.of(Schema.Type.INT)))
    val schema = Schema.recordOf("record", fields)
    val formatSpec = new FormatSpecification("csv", schema)
    val rdd: RDD[(Long, GenericStreamEventData[StructuredRecord])] = sc.fromStream(streamName, formatSpec)

    import sqlContext.implicits._
    rdd.values.map(_.getBody).map(r => new Person(r.get("name"), r.get("age"))).toDF().registerTempTable("people")

    // Execute a SQL on the table and save the result
    sqlContext.sql(sec.getRuntimeArguments.get("sql.statement"))
      .map(r => (r.getString(0), r.getInt(1)))
      .repartition(1)
      .saveAsDataset(sec.getRuntimeArguments.get("output.dataset"))
  }
}
