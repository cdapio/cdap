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

import co.cask.cdap.api.spark.AbstractExtendedSpark
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.api.spark.SparkMain
import com.google.common.io.BaseEncoding
import org.apache.spark.SparkContext

import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files

import scala.collection.JavaConversions._

/**
  *
  */
class ScalaDynamicSpark extends AbstractExtendedSpark with SparkMain {

  val classSource =
    """
      package test.dynamic

      import co.cask.cdap.api.common._
      import co.cask.cdap.api.spark._
      import org.apache.spark._

      object Compute {
       def run(sc: SparkContext)(implicit sec: SparkExecutionContext) {
         // Creates a dummy SparkMain instance for importing implicits
         val sparkMain = new SparkMain() { override def run(implicit sec: SparkExecutionContext): Unit = { } }
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

  override protected def configure(): Unit = {
    setMainClass(classOf[ScalaDynamicSpark])
    val compiler = getConfigurer.createSparkCompiler()
    try {
      // Compile the code and remember it in the property.
      compiler.compile(classSource)
      val bos = new ByteArrayOutputStream()
      try {
        compiler.saveAsJar(bos)
      } finally {
        bos.close()
      }
      setProperties(Map(("compiled.jar", BaseEncoding.base64().encode(bos.toByteArray))))
    } finally {
      compiler.close()
    }
  }

  override def run(implicit sec: SparkExecutionContext): Unit = {
    val sc = new SparkContext

    val depJar = new File(sec.getRuntimeArguments.get("tmpdir"), "compiled.jar")
    Files.write(depJar.toPath, BaseEncoding.base64().decode(sec.getSpecification.getProperty("compiled.jar")))

    val intp = sec.createInterpreter()
    try {
      intp.addDependencies(depJar)
      intp.addImports("test.dynamic.Compute")
      intp.bind("sc", sc)
      intp.bind("sec", sec.getClass.getName, sec, "implicit")
      intp.interpret("Compute.run(sc)");
    } finally {
      intp.close()
    }
  }
}
