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
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.data.stream.StreamBatchReadable;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

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
    createDataset("latest", KeyValueTable.class);
    addMapReduce(new BodyTracker());
  }

  public static final class BodyTracker extends AbstractMapReduce {
    @Override
    public void configure() {
      setOutputDataset("latest");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(StreamMapper.class);
      job.setNumReduceTasks(0);
      job.setMapOutputKeyClass(LongWritable.class);
      job.setMapOutputValueClass(StructuredRecord.class);
      FormatSpecification formatSpec = new FormatSpecification(
        Formats.STRING,
        Schema.recordOf("event", Schema.Field.of("body", Schema.of(Schema.Type.STRING))),
        Collections.<String, String>emptyMap()
      );
      StreamBatchReadable.useStreamInput(context, "mrStream", 0, Long.MAX_VALUE, formatSpec);
    }
  }

  // reads input from the stream and records the last timestamp that the body was seen
  public static class StreamMapper extends Mapper<LongWritable, StructuredRecord, byte[], byte[]> {

    @Override
    public void map(LongWritable key, StructuredRecord streamEvent, Context context)
      throws IOException, InterruptedException {
      context.write(Bytes.toBytes((String) streamEvent.get("body")), Bytes.toBytes(key.get()));
    }
  }

}
