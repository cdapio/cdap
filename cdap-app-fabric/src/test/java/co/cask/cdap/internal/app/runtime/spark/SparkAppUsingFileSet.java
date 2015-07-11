/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkProgram;
import co.cask.cdap.api.spark.SparkContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * A dummy app with spark program which counts the characters in a string
 */
public class SparkAppUsingFileSet extends AbstractApplication {
  @Override
  public void configure() {
    try {
      setName("SparkAppUsingFileSet");
      setDescription("Application with Spark program using fileset input/output");
      createDataset("count", KeyValueTable.class);
      createDataset("fs", FileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      createDataset("pfs", PartitionedFileSet.class, PartitionedFileSetProperties.builder()
        .setPartitioning(Partitioning.builder().addStringField("x").build())
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      createDataset("tpfs", TimePartitionedFileSet.class, FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class)
        .setOutputProperty(TextOutputFormat.SEPERATOR, ":").build());
      addSpark(new CharCountSpecification());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  public static final class CharCountSpecification extends AbstractSpark {
    @Override
    public void configure() {
      setName("SparkCharCountProgram");
      setDescription("Use Objectstore dataset as input job");
      setMainClass(CharCountProgram.class);
    }
  }

  public static class CharCountProgram implements JavaSparkProgram {
    @Override
    public void run(SparkContext context) {
      String dataset = context.getRuntimeArguments().get("input");

      // read the dataset
      JavaPairRDD<LongWritable, Text> inputData = context.readFromDataset(dataset, LongWritable.class, Text.class);

      // create a new RDD with the same key but with a new value which is the length of the string
      JavaPairRDD<byte[], byte[]> stringLengths = inputData.mapToPair(
        new PairFunction<Tuple2<LongWritable, Text>, byte[], byte[]>() {
        @Override
        public Tuple2<byte[], byte[]> call(Tuple2<LongWritable, Text> pair) throws Exception {
          return new Tuple2<>(Bytes.toBytes(pair._1().get()), Bytes.toBytes(pair._2().getLength()));
        }
      });

      // write the character count to dataset
      context.writeToDataset(stringLengths, "count", byte[].class, byte[].class);
      ((JavaSparkContext) context.getOriginalSparkContext()).stop();
    }
  }
}
