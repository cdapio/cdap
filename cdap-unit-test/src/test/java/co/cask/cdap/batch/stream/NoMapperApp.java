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

package co.cask.cdap.batch.stream;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * A Test application that has one MapReduce which doesn't have Mapper.
 */
public class NoMapperApp extends AbstractApplication {

  @Override
  public void configure() {
    addStream(new Stream("nomapper"));
    createDataset("results", KeyValueTable.class);
    addMapReduce(new NoMapperMapReduce());
  }

  /**
   * A MapReduce without a Mapper.
   */
  public static final class NoMapperMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      useStreamInput("nomapper");
      setOutputDataset("results");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setReducerClass(NoMapperReducer.class);
    }
  }

  /**
   * Reducer to read from stream and write to the output KeyValueTable with stream body as both key and value.
   */
  public static final class NoMapperReducer extends Reducer<LongWritable, Text, byte[], byte[]> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
      for (Text value : values) {
        byte[] bytes = value.copyBytes();
        context.write(bytes, bytes);
      }
    }
  }
}
