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
import co.cask.cdap.api.spark.{AbstractSpark, SparkExecutionContext, SparkMain}
import org.apache.spark.SparkContext

/**
  *
  */
class ScalaDynamicSpark extends AbstractSpark with SparkMain {

  override protected def configure(): Unit = {
    setMainClass(classOf[ScalaDynamicSpark])
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val interpreter = sec.createInterpreter()
    try {
      val classSource =
        """
           import co.cask.cdap.api.common._
           import co.cask.cdap.api.spark._
           import org.apache.spark._

           object Compute {
             def run(sc: SparkContext, sparkMain: SparkMain)(implicit sec: SparkExecutionContext) {
               import sparkMain._

               val args = sec.getRuntimeArguments()
               sc.fromStream[String](args.get("input"))
                 .flatMap(_.split("\\s+"))
                 .map((_, 1))
                 .reduceByKey(_ + _)
                 .map(t => (Bytes.toBytes(t._1), Bytes.toBytes(t._2)))
                 .saveAsDataset(args.get("output")
               )
             }
           }
        """
      interpreter.compile(classSource)

      interpreter.bind("sc", sc)
      interpreter.bind("sparkMain", this)
      interpreter.bind("sec", sec)
      interpreter.interpret("Compute.run(sc, sparkMain)(sec)");
    } finally {
      interpreter.close()
    }
  }
}
