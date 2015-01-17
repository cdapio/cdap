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

package co.cask.cdap.mapreduce;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.stream.GenericStreamEventData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;

/**
 * App used to test whether M/R can read from streams.
 */
public class AppWithMapReduceUsingStream extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingStream");
    setDescription("Application with MapReduce job using stream as input");
    addStream(new Stream("mrStream"));
    createDataset("prices", KeyValueTable.class);
    addMapReduce(new BodyTracker());
  }

  public static final class BodyTracker extends AbstractMapReduce {
    @Override
    public void configure() {
      setOutputDataset("prices");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(TickerMapper.class);
      job.setReducerClass(PriceCounter.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(FloatWritable.class);
      job.setOutputKeyClass(byte[].class);
      job.setOutputValueClass(byte[].class);
      Schema schema = Schema.recordOf(
        "event",
        Schema.Field.of("ticker", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("num_traded", Schema.of(Schema.Type.INT)),
        Schema.Field.of("price", Schema.of(Schema.Type.FLOAT))
      );
      FormatSpecification formatSpec = new FormatSpecification(
        Formats.AVRO,
        schema,
        Collections.<String, String>emptyMap()
      );
      StreamBatchReadable.useStreamInput(context, "mrStream", 0, Long.MAX_VALUE, formatSpec);
    }
  }

  // reads input from the stream as avro and calculates the total prices of all stocks traded
  public static class TickerMapper extends
    Mapper<LongWritable, GenericStreamEventData<GenericRecord>, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, GenericStreamEventData<GenericRecord> eventData, Context context)
      throws IOException, InterruptedException {
      GenericRecord body = eventData.getBody();
      String ticker = body.get("ticker").toString();
      Integer numTraded = (Integer) body.get("num_traded");
      Float price = (Float) body.get("price");
      context.write(new Text(ticker), new FloatWritable(numTraded * price));
    }
  }

  // reads input from the stream and records the last timestamp that the body was seen
  public static class PriceCounter extends Reducer<Text, FloatWritable, byte[], byte[]> {

    @Override
    public void reduce(Text key, Iterable<FloatWritable> prices, Context context)
      throws IOException, InterruptedException {
      Float totalPrice = 0f;
      for (FloatWritable price : prices) {
        totalPrice += price.get();
      }
      context.write(key.getBytes(), Bytes.toBytes(totalPrice));
    }
  }
}
