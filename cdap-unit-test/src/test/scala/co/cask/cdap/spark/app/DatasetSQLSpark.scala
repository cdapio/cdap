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

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * A Spark program for testing Dataset to DataFrame conversion
  */
class DatasetSQLSpark extends AbstractSpark with SparkMain {

  override protected def configure() = {
    setMainClass(classOf[DatasetSQLSpark])
  }

  override def run(implicit sec: SparkExecutionContext) = {
    val sc = new SparkContext
    val sql = new SQLContext(sc)

    import sql.implicits._
    sc.fromDataset[Array[Byte], Person]("PersonTable").values.toDF().registerTempTable("Person")

    val v = sql.sql("SELECT * FROM Person WHERE age > 10")
       .map(r =>
         (Bytes.toBytes("new:" + r.getAs[String]("id")), new Person(r.getAs("id"), r.getAs("name"), r.getAs("age"))))
        .saveAsDataset("PersonTable")
  }
}
