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

import java.io.File
import java.util.concurrent.TimeUnit

import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import co.cask.cdap.api.workflow.{Value, WorkflowForkNode}
import com.google.common.base.Stopwatch
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * A Spark program for testing forking Spark in workflow.
  */
class ForkSpark(name: String) extends AbstractSpark with SparkMain {

  def this() = this(classOf[ForkSpark].getSimpleName)

  override protected def configure(): Unit = {
    setName(name)
    setMainClass(classOf[ForkSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    // Touch a file in a directory provided by the runtime argument
    // Then block until there are "n" files in that directory
    // Where n is the number of branches in the fork
    // This acts as a simple barrier for all spark programs in the fork
    val barrierDir = new File(sec.getRuntimeArguments.get("barrier.dir"))
    val file = new File(barrierDir, sec.getRunId.getId)
    require(file.createNewFile())
    val branchSize = getBranchSize(sec)
    val stopWatch = new Stopwatch().start()
    while (barrierDir.list().length < branchSize && stopWatch.elapsedTime(TimeUnit.SECONDS) < 10) {
      TimeUnit.MILLISECONDS.sleep(100)
    }

    val conf = new SparkConf
    val sc = new SparkContext(conf)

    // Just run something in Spark, base on some property that we know is set by SparkSubmit to system properties.
    // If fork support is implemented correctly, then those values from the SparkConf would be different for
    // different Spark job in the branch; otherwise they'll be the same due to the barrier in place above
    val sum = sc.parallelize(conf.get("spark.app.name").toCharArray)
                .map(_.toInt)
                .reduce(_ + _)

    sec.getWorkflowToken.foreach(_.put("sum", Value.of(sum)))
  }

  private def getBranchSize(sec: SparkExecutionContext): Int = {
    val workflowSpec = sec.getApplicationSpecification.getWorkflows.get(sec.getWorkflowInfo.get.getName)
    workflowSpec.getNodes.collect { case node: WorkflowForkNode => node }.head.getBranches.size()
  }
}
