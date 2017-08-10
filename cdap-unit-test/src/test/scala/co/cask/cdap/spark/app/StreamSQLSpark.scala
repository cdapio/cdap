/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.AbstractSpark
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
  * Unit test for various functionality that involve running Spark SQL on Stream.
  */
class StreamSQLSpark extends AbstractSpark with SparkMain {

  override protected def configure(): Unit = {
    setMainClass(classOf[StreamSQLSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext
    val sql = new SQLContext(sc)

    import sql.implicits._

    val streamName = sec.getRuntimeArguments.get("input.stream")

    // Do a full scan
    val allRecords = sql.sql(s"SELECT * FROM cdapstream.`${streamName}`")
    // Register a temp table InputStream as well.
    allRecords.registerTempTable("InputStream")

    // Validate the schema
    val streamSchema = StructType(Seq(
      StructField("ts", DataTypes.LongType, false),
      StructField("headers", MapType(DataTypes.StringType, DataTypes.StringType, false), false),
      StructField("firstName", DataTypes.StringType, false),
      StructField("lastName", DataTypes.StringType, false),
      StructField("age", DataTypes.IntegerType, false)
    ))
    require(streamSchema == allRecords.schema, "Stream Schema is not as expected: " + allRecords.schema)

    // Check we get all results
    require(allRecords.count() == 6, "Expected 6 records")

    // Collects all the timestamp. Use it for testing timestamp filter
    val timestamps = allRecords.collect().map(_.getAs[Long]("ts"))

    // Test some timestamp filter. More extensive tests are tested separatedly in StreamRelationTest
    // Use the IN filter for timestamp. This will generate multiple RDDs concat together
    val byTimestamps =
      sql.sql("SELECT ts FROM InputStream WHERE ts IN (" + timestamps.slice(2, 4).mkString(",") + ")")
         .collect()
    require(byTimestamps.length == 2, "Expected two records")
    require(byTimestamps.map(_.getAs[Long]("ts")).sameElements(timestamps.slice(2, 4)),
            "Timestamps are not the same")

    // Query with simple condition
    val lessThan30 = sql.sql("SELECT age FROM InputStream WHERE age < 30").collect()
    require(lessThan30.forall(r => r.getAs[Int]("age") < 30), "Expects all ages to be < 30")

    // Select with a AND filter that involves timestamp
    val filtered30 = sql.sql(s"SELECT * FROM InputStream WHERE ts > ${timestamps.head} AND age < 30").collect()
    // Expected the first record (which has age < 30) is filtered out because of the timestamp
    require(filtered30.length == lessThan30.length - 1)
    require(filtered30.forall(r => r.getAs[Int]("age") < 30), "Expects all ages to be < 30")

    // Register a different table with different schema
    val newDF = sql.read
      .format("cdapstream")
      .schema(StructType(Seq(
        StructField("fname", DataTypes.StringType, true),
        StructField("lname", DataTypes.StringType, true),
        StructField("age", DataTypes.ShortType, false)
      )))
      .option("timestamp.column.name", "timestamp")
      .option("headers.column.enabled", "false")
      .load(streamName)
    newDF.registerTempTable("NewInput")

    // Joining the tables
    val joinResult =
      sql.sql("SELECT count(1), lname FROM InputStream as A, NewInput as B " +
              "WHERE A.lastName = B.lname AND A.firstName != B.fname AND A.ts = B.timestamp GROUP BY B.lname")
    // Only last name "Thomson" and "Edison" has two records
    require(joinResult.collect().forall(r => {
      r.getAs[Long](0) == 2 && Set("Thomson", "Edison").contains(r.getAs[String](1))
    }))

    // Raw event query
    val rawDF =
      sql.read.format("cdapstream").option("stream.format", "raw").load(streamName)
    rawDF.registerTempTable("RawTable")

    val rawDecodedDF = sql
      .sql("SELECT body FROM RawTable")
      .map(r => {
        val body = Bytes.toString(r.getAs[Array[Byte]](0))
        val splits = body.split(",")
        Person(splits(0), splits(1), splits(2).toInt)
      }).toDF
    rawDecodedDF.registerTempTable("RawDecoded")

    // Join the raw table with the NewInput table
    val rawJoinResult =
      sql.sql("SELECT count(1), lname FROM RawDecoded as A, NewInput as B " +
        "WHERE A.lastName = B.lname AND A.firstName != B.fname GROUP BY B.lname")
    // Only last name "Thomson" and "Edison" has two records
    require(joinResult.collect().forall(r => {
      r.getAs[Long](0) == 2 && Set("Thomson", "Edison").contains(r.getAs[String](1))
    }))

    sc.stop()
  }

  /**
    * Case class for decoding stream events
    */
  case class Person(firstName: String, lastName: String, age: Int)
}
