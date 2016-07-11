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

package co.cask.cdap.spark.stream;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * A dummy app with spark program which simply read from a dataset from different namespace and write to a dataset in
 * the current namespace
 */
public class TestSparkCrossNSDatasetApp extends AbstractApplication {
  public static final String SOURCE_DATA_NAMESPACE = "dataSpaceForSpark";

  @Override
  public void configure() {
    setName("TestSparkCrossNSDatasetApp");
    setDescription("App to test Spark with Datasets from other namespace");
    addStream(new Stream("testStream"));
    createDataset("result", KeyValueTable.class);
    addSpark(new SparkStreamProgramSpec());
  }

  public static class SparkStreamProgramSpec extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkStreamProgram");
      setDescription("Test Spark with Datasets from other namespace");
      setMainClass(SparkStreamProgram.class);
    }
  }

  public static class SparkStreamProgram implements JavaSparkMain {
    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<byte[], byte[]> rdd = sec.fromDataset(SOURCE_DATA_NAMESPACE, "result");
      sec.saveAsDataset(rdd, "result");
    }
  }
}
