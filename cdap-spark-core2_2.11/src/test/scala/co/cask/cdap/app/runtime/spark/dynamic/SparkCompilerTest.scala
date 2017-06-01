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

package co.cask.cdap.app.runtime.spark.dynamic

import java.net.URL

import co.cask.cdap.api.spark.dynamic.{SparkCompiler, SparkInterpreter}

import scala.tools.nsc.Settings

/**
  * Unit test for SparkCompiler and SparkInterpreter.
  */
class SparkCompilerTest extends SparkCompilerTestBase {

  override protected def createCompiler(): SparkCompiler = {
    val settings = AbstractSparkCompiler.setClassPath(new Settings(), getClass.getClassLoader)
    new DefaultSparkCompiler(settings, new URLAdder {
      override def addURLs(urls: URL*) = {
        // no-op
      }
    }, () => { });
  }

  override protected def createInterpreter(): SparkInterpreter = {
    val settings = AbstractSparkCompiler.setClassPath(new Settings(), getClass.getClassLoader)
    settings.Yreploutdir.value = SparkCompilerTestBase.TEMP_FOLDER.newFolder.getAbsolutePath
    new DefaultSparkInterpreter(settings, new URLAdder {
      override def addURLs(urls: URL*) = {
        // no-op
      }
    }, () => { });
  }
}
