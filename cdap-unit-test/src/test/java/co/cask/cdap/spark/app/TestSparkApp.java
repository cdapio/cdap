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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;

/**
 * An application for testing various behavior of the Spark runtime.
 */
public class TestSparkApp extends AbstractApplication {

  @Override
  public void configure() {

    addStream(new Stream("SparkStream"));
    createDataset("SparkResult", KeyValueTable.class);
    createDataset("SparkThresholdResult", KeyValueTable.class);

    addSpark(new ClassicSpark());
    addSpark(new ScalaClassicSpark());
    addSpark(new ExplicitTransactionSpark());
  }

  public static final class ClassicSpark extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(ClassicSparkProgram.class);
    }
  }

  public static final class ScalaClassicSpark extends AbstractSpark {
    @Override
    protected void configure() {
      setMainClass(ScalaClassicSparkProgram.class);
    }
  }
}
