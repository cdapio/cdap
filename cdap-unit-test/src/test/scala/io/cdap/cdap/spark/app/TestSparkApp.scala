/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.spark.app


import io.cdap.cdap.api.annotation.{Property, UseDataSet}
import io.cdap.cdap.api.app.{AbstractApplication, ProgramType}
import io.cdap.cdap.api.common.Bytes
import io.cdap.cdap.api.customaction.AbstractCustomAction
import io.cdap.cdap.api.data.schema.Schema
import io.cdap.cdap.api.dataset.lib._
import io.cdap.cdap.api.spark.AbstractSpark
import io.cdap.cdap.api.workflow.AbstractWorkflow
import io.cdap.cdap.api.{Config, ProgramStatus}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import scala.collection.JavaConversions._

/**
  * An application for SparkTestRun.
  */
class TestSparkApp extends AbstractApplication[Config] {

  override def configure() = {
    createDataset("ResultTable", classOf[KeyValueTable])
    createDataset("KeyValueTable", classOf[KeyValueTable])
    createDataset("SparkResult", classOf[KeyValueTable])
    createDataset("SparkThresholdResult", classOf[KeyValueTable])
    createDataset("PeopleFileSet", classOf[FileSet], FileSetProperties.builder
        .setOutputFormat(classOf[TextOutputFormat[_, _]])
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
        .build)
    createDataset("TimeSeriesResult", classOf[TimeseriesTable])

    createDataset("PersonTable", classOf[ObjectMappedTable[Person]],
                  ObjectMappedTableProperties.builder()
                    .setType(classOf[Person])
                    .setRowKeyExploreName("id")
                    .setRowKeyExploreType(Schema.Type.STRING)
                    .build())

    addSpark(new DatasetSQLSpark)

    addSpark(new ClassicSpark)
    addSpark(new ScalaClassicSpark)
    addSpark(new TransactionSpark)
    addSpark(new PythonSpark)

    addSpark(new KafkaSparkStreaming)

    addSpark(new ScalaDynamicSpark)

    addSpark(new SparkServiceProgram)

    addSpark(new ForkSpark("ForkSpark1"))
    addSpark(new ForkSpark("ForkSpark2"))

    addSpark(new TriggeredSpark)

    addWorkflow(new ForkSparkWorkflow)

    addWorkflow(new TriggeredWorkflow)
    schedule(buildSchedule("schedule", ProgramType.WORKFLOW, classOf[TriggeredWorkflow].getSimpleName)
      .triggerOnProgramStatus(ProgramType.SPARK, classOf[ScalaClassicSpark].getSimpleName, ProgramStatus.COMPLETED))
  }

  final class ClassicSpark extends AbstractSpark {

    @UseDataSet("ResultTable")
    var resultTable: KeyValueTable = _
    @Property
    val mainClassName = classOf[ClassicSparkProgram].getName

    override protected def configure {
      setMainClassName(mainClassName)
    }

    override def destroy() {
      resultTable.increment(Bytes.toBytes(mainClassName),
                            if (getContext.getState.getStatus eq ProgramStatus.COMPLETED) 1 else 0)
    }
  }

  final class ScalaClassicSpark extends AbstractSpark {

    @UseDataSet("ResultTable")
    var resultTable: KeyValueTable = _
    @Property
    val mainClassName = "io.cdap.cdap.spark.app.ScalaClassicSparkProgram"

    override protected def configure {
      setMainClassName(mainClassName)
    }

    override def destroy() {
      resultTable.increment(Bytes.toBytes(mainClassName),
                            if (getContext.getState.getStatus eq ProgramStatus.COMPLETED) 1 else 0)
    }
  }

  final class ForkSparkWorkflow extends AbstractWorkflow {
    override protected def configure(): Unit = {
      fork()
        .addSpark("ForkSpark1")
      .also()
        .addSpark("ForkSpark2")
      .join()
      addAction(new VerifyAction)
    }
  }

  final class VerifyAction extends AbstractCustomAction {
    override def run() = {
      val values = getContext.getWorkflowToken.getAll("sum")
      require(values.map(_.getValue.getAsInt).distinct.size == 2,
              "Expect number of distinct 'sum' token be 2: " + values)
    }
  }

  final class TriggeredWorkflow extends AbstractWorkflow {
    override def configure(): Unit = {
      setDescription("TriggeredWorkflow description")
      addSpark(classOf[TriggeredSpark].getSimpleName)
    }
  }
}
