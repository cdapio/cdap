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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import com.google.common.base.Strings;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

/**
 * A dummy app with spark program which reads from a dataset in different namespace and write to a dataset in  a
 * different namespace if they have been provided through runtime arguments else it defaults to its own namespace.
 */
public class TestSparkCrossNSDatasetApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("TestSparkCrossNSDatasetApp");
    setDescription("App to test Spark with Datasets from other namespace");
    createDataset("outputDataset", KeyValueTable.class);
    addSpark(new SparkCrossNSDatasetProgramSpec());
  }

  public static class SparkCrossNSDatasetProgramSpec extends AbstractSpark {
    @Override
    public void configure() {
      setName(SparkCrossNSDatasetProgram.class.getSimpleName());
      setDescription("Test Spark with Datasets from other namespace");
      setMainClass(SparkCrossNSDatasetProgram.class);
    }
  }

  public static class SparkCrossNSDatasetProgram implements JavaSparkMain {

    public static final String INPUT_DATASET_NAMESPACE = "input.dataset.namespace";
    public static final String INPUT_DATASET_NAME = "input.dataset.name";
    public static final String OUTPUT_DATASET_NAMESPACE = "output.dataset.namespace";
    public static final String OUTPUT_DATASET_NAME = "output.dataset.name";

    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      Map<String, String> runtimeArguments = sec.getRuntimeArguments();
      String inputDatasetNS = Strings.isNullOrEmpty(runtimeArguments.get(INPUT_DATASET_NAMESPACE)) ?
        sec.getNamespace() : runtimeArguments.get(INPUT_DATASET_NAMESPACE);
      String inputDatasetName = Strings.isNullOrEmpty(runtimeArguments.get(INPUT_DATASET_NAME)) ?
        "inputDataset" : runtimeArguments.get(INPUT_DATASET_NAME);
      String outputDatasetNS = Strings.isNullOrEmpty(runtimeArguments.get(OUTPUT_DATASET_NAMESPACE)) ?
        sec.getNamespace() : runtimeArguments.get(OUTPUT_DATASET_NAMESPACE);
      String outputDatasetName = Strings.isNullOrEmpty(runtimeArguments.get(OUTPUT_DATASET_NAME)) ?
        "outputDataset" : runtimeArguments.get(OUTPUT_DATASET_NAME);

      JavaPairRDD<byte[], byte[]> rdd = sec.fromDataset(inputDatasetNS, inputDatasetName);
      sec.saveAsDataset(rdd, outputDatasetNS, outputDatasetName);
    }
  }
}
