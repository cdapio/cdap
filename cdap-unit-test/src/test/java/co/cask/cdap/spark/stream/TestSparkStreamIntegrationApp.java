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
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
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

  public static class SparkStreamProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      JavaPairRDD<LongWritable, Text> rdd = context.readFromStream("testStream", Text.class);
      JavaPairRDD<byte[], byte[]> resultRDD = rdd.mapToPair(new PairFunction<Tuple2<LongWritable, Text>,
        byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
          return new Tuple2<byte[], byte[]>(Bytes.toBytes(longWritableTextTuple2._2().toString()),
                                            Bytes.toBytes(longWritableTextTuple2._2().toString()));
        }
      });
      context.writeToDataset(resultRDD, "result", byte[].class, byte[].class);
    }
  }
}
