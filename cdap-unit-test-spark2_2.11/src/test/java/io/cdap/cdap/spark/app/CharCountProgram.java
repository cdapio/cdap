/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.cdap.api.spark.SparkClientContext;
import com.google.common.base.Preconditions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 */
public class CharCountProgram extends AbstractSpark implements JavaSparkMain {

  @Override
  protected void configure() {
    setMainClass(CharCountProgram.class);
  }

  @Override
  public void initialize() throws Exception {
    SparkClientContext context = getContext();
    context.setSparkConf(new SparkConf().set("spark.io.compression.codec",
                                             "org.apache.spark.io.LZFCompressionCodec"));

    Table totals = context.getDataset("totals");
    totals.get(new Get("total").add("total")).getLong("total");
    totals.put(new Put("total").add("total", 0L));
  }

  @Override
  public void run(final JavaSparkExecutionContext sec) throws Exception {
    JavaSparkContext sc = new JavaSparkContext();

    // Verify the codec is being set
    Preconditions.checkArgument(
      "org.apache.spark.io.LZFCompressionCodec".equals(sc.getConf().get("spark.io.compression.codec")));

    // read the dataset
    JavaPairRDD<byte[], String> inputData = sec.fromDataset("keys");

    // create a new RDD with the same key but with a new value which is the length of the string
    final JavaPairRDD<byte[], byte[]> stringLengths = inputData.mapToPair(new PairFunction<Tuple2<byte[], String>,
      byte[], byte[]>() {
      @Override
      public Tuple2<byte[], byte[]> call(Tuple2<byte[], String> stringTuple2) throws Exception {
        return new Tuple2<>(stringTuple2._1(), Bytes.toBytes(stringTuple2._2().length()));
      }
    });

    // write a total count to a table (that emits a metric we can validate in the test case)
    sec.execute(new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        long count = stringLengths.count();
        Table totals = context.getDataset("totals");
        totals.increment(new Increment("total").add("total", count));

        // write the character count to dataset
        sec.saveAsDataset(stringLengths, "count");
      }
    });
  }
}
