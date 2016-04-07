/*
 * Copyright © 2014 Cask Data, Inc.
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
 * A dummy app with spark program which counts the characters in a string read from a Stream
 */
public class TestSparkStreamIntegrationApp extends AbstractApplication {
  @Override
  public void configure() {
    setName("TestSparkStreamIntegrationApp");
    setDescription("App to test Spark with Streams");
    addStream(new Stream("testStream"));
    createDataset("result", KeyValueTable.class);
    addSpark(new SparkStreamProgramSpec());
  }

  public static class SparkStreamProgramSpec extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkStreamProgram");
      setDescription("Test Spark with Streams");
      setMainClass(SparkStreamProgram.class);
    }
  }

  public static class SparkStreamProgram implements JavaSparkMain {
    @Override
    public void run(JavaSparkExecutionContext sec) throws Exception {
      JavaSparkContext jsc = new JavaSparkContext();
      JavaPairRDD<Long, String> rdd = sec.fromStream("testStream", String.class);
      JavaPairRDD<byte[], byte[]> resultRDD = rdd.mapToPair(new PairFunction<Tuple2<Long, String>,
        byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<Long, String> tuple2) throws Exception {
          return new Tuple2<>(Bytes.toBytes(tuple2._2()), Bytes.toBytes(tuple2._2()));
        }
      });
      sec.saveAsDataset(resultRDD, "result");
    }
  }
}
