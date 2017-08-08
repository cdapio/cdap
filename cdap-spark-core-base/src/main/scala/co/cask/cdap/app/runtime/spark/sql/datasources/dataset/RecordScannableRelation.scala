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

package co.cask.cdap.app.runtime.spark.sql.datasources.dataset

import java.util

import co.cask.cdap.api.data.batch.{RecordScannable, Split, Splits}
import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.UnsupportedTypeException
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.api.spark.sql.DataFrames
import co.cask.cdap.app.runtime.spark.SparkClassLoader
import co.cask.cdap.app.runtime.spark.data.RecordScannableRDD
import co.cask.cdap.proto.id.DatasetId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  *
  */
private[dataset] class RecordScannableRelation(override val sqlContext: SQLContext,
                                               override val schema: StructType,
                                               datasetId: DatasetId,
                                               parameters: Map[String, String])
  extends BaseRelation with Serializable with PrunedFilteredScan {

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val sparkClassLoader = SparkClassLoader.findFromContext()
    val sec = sparkClassLoader.getSparkExecutionContext(false)

    // Creates the RDD[Row] based on the RecordScannable
    sec.createDatasetCompute()(datasetId.getNamespace, datasetId.getDataset, parameters, (dataset: Dataset) => {
      val sc = sqlContext.sparkContext
      val driveHttpServiceURI = sec.getDriveHttpServiceBaseURI(sc)
      // Create a target schema based on the query columns
      val rowSchema = StructType(requiredColumns.map(col => schema.fields(schema.fieldIndex(col))))
      // User may provide a custom set of splits from the query parameters
      val inputSplits = parameters.get("input.splits")
        .map(Splits.decode(_, new util.ArrayList[Split](), sparkClassLoader))

      dataset.asInstanceOf[RecordScannable[_]].getRecordType match {
        case recordType if classOf[StructuredRecord] == recordType => {
          val recordScannable = dataset.asInstanceOf[RecordScannable[StructuredRecord]]
          new RecordScannableRDD[StructuredRecord](sc, datasetId.getNamespace, datasetId.getDataset, parameters,
                                                   inputSplits.getOrElse(recordScannable.getSplits),
                                                   driveHttpServiceURI)
            .map(DataFrames.toRow(_, rowSchema))
        }
        case beanType: Class[_] => {
          val recordScannable = dataset.asInstanceOf[RecordScannable[_]]
          val rdd = new RecordScannableRDD(sc, datasetId.getNamespace, datasetId.getDataset, parameters,
                                 inputSplits.getOrElse(recordScannable.getSplits),
                                 driveHttpServiceURI)(ClassTag(beanType))
          sqlContext.createDataFrame(rdd, beanType).rdd
        }
        case anyType =>
          throw new UnsupportedTypeException(s"Dataset $datasetId has record type $anyType is not supported")
      }
    })
  }
}
