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

package co.cask.cdap.spark;

import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class FileCountSparkProgram extends AbstractSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setMainClass(FileCountSparkProgram.class);
  }

  @Override
  public void run(JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext jsc = new JavaSparkContext();

    String input = sec.getRuntimeArguments().get("input");
    String output = sec.getRuntimeArguments().get("output");

    // read the dataset
    JavaPairRDD<Long, String> inputData = sec.fromDataset(input);

    JavaPairRDD<String, Integer> stringLengths = transformRDD(inputData);

    // write the character count to dataset
    sec.saveAsDataset(stringLengths, output);

    String inputPartitionTime = sec.getRuntimeArguments().get("inputKey");
    String outputPartitionTime = sec.getRuntimeArguments().get("outputKey");

    // read and write datasets with dataset arguments
    if (inputPartitionTime != null && outputPartitionTime != null) {
      Map<String, String> inputArgs = new HashMap<>();
      TimePartitionedFileSetArguments.setInputStartTime(inputArgs, Long.parseLong(inputPartitionTime) - 100);
      TimePartitionedFileSetArguments.setInputEndTime(inputArgs, Long.parseLong(inputPartitionTime) + 100);

      // read the dataset with user custom dataset args
      JavaPairRDD<Long, String> customPartitionData = sec.fromDataset(input, inputArgs);

      // create a new RDD with the same key but with a new value which is the length of the string
      JavaPairRDD<String, Integer> customPartitionStringLengths = transformRDD(customPartitionData);

      // write the character count to dataset with user custom dataset args
      Map<String, String> outputArgs = new HashMap<>();
      TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, Long.parseLong(outputPartitionTime));
      sec.saveAsDataset(customPartitionStringLengths, output, outputArgs);
    }
  }

  private JavaPairRDD<String, Integer> transformRDD(JavaPairRDD<Long, String> inputData) {
    // create a new RDD with the same key but with a new value which is the length of the string
    return inputData.mapToPair(
      new PairFunction<Tuple2<Long, String>, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(Tuple2<Long, String> pair) throws Exception {
          return new Tuple2<>(pair._2(), pair._2().length());
        }
      });
  }
}
