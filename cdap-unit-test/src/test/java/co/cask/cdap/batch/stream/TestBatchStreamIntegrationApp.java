/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.batch.stream;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Flow stream integration tests.
 */
public class TestBatchStreamIntegrationApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("TestFlowStreamIntegrationApp");
    addStream(new Stream("s_1"));
    createDataset("results", KeyValueTable.class);
    addMapReduce(new StreamTestBatch());
    addMapReduce(new StreamTestBatchIdDecoder());
  }

  public static class StreamTestBatch extends AbstractMapReduce {

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      Job job = context.getHadoopJob();
      setMapperClass(job);
      job.setReducerClass(StreamTestBatchReducer.class);
      context.addInput(Input.ofStream("s_1"));
      context.addOutput(Output.ofDataset("results"));
    }

    protected void setMapperClass(Job job) {
      job.setMapperClass(StreamTestBatchMapper.class);
    }
  }

  public static class StreamTestBatchIdDecoder extends StreamTestBatch {
    @Override
    protected void setMapperClass(Job job) {
      job.setMapperClass(StreamTestBatchIdDecoderMapper.class);
    }
  }

  public static class StreamTestBatchMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    @Override
    protected void map(LongWritable key, BytesWritable value,
                       Context context) throws IOException, InterruptedException {
      Text output = new Text(value.copyBytes());
      context.write(output, output);
    }
  }

  public static class StreamTestBatchIdDecoderMapper extends Mapper<LongWritable, StreamEvent, Text, Text> {
    @Override
    protected void map(LongWritable key, StreamEvent value,
                       Context context) throws IOException, InterruptedException {
      Text output = new Text(value.getBody().array());
      context.write(output, output);
    }
  }

  public static class StreamTestBatchReducer extends Reducer<Text, Text, byte[], byte[]> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        byte[] bytes = value.copyBytes();
        context.write(bytes, bytes);
      }
    }
  }
}
