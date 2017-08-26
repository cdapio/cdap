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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;

/**
 * A {@link Spark} program that runs Python.
 */
public class PythonSpark extends AbstractSpark {

  @Override
  protected void initialize() throws Exception {
    getContext().setPySparkScript(getClass().getClassLoader().getResource("testPySpark.py").toURI());
  }
}
