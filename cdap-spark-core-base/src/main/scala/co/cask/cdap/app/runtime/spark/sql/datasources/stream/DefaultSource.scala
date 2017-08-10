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

package co.cask.cdap.app.runtime.spark.sql.datasources.stream

import co.cask.cdap.app.runtime.spark.SparkRuntimeContextProvider
import co.cask.cdap.proto.id.NamespaceId
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType

/**
  * The Spark data source for Stream.
  */
class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister {

  override def shortName(): String = {
    "cdapstream"
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    return createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation = {
    val namespace = parameters.get("namespace")
    val path = parameters.get("path")

    val runtimeContext = SparkRuntimeContextProvider.get()
    val programRunId = runtimeContext.getProgramRunId

    val streamId = path match {
      case Some(streamName) => {
        namespace match {
          case Some(ns) => new NamespaceId(ns).stream(streamName)
          case _ => programRunId.getNamespaceId.stream(streamName)
        }
      }
      case _ => throw new IllegalArgumentException("Missing stream name, which is derived from the 'path' parameter")
    }

    new StreamRelation(sqlContext, streamId, Option(schema), parameters, runtimeContext)
  }
}
