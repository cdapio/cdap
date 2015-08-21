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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class AppWithMapReduceUsingObjectStore extends AbstractApplication {
  @Override
  public void configure() {
    try {
      setName("AppWithMapReduceObjectStore");
      setDescription("Application with MapReduce job using objectstore as dataset");
      createDataset("count", KeyValueTable.class);
      ObjectStores.createObjectStore(getConfigurer(), "keys", String.class);
      addMapReduce(new ComputeCounts());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  /**
   *
   */
  public static final class ComputeCounts extends AbstractMapReduce {
    @Override
    public void configure() {
      setInputDataset("keys");
      setOutputDataset("count");
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = context.getHadoopJob();
      job.setMapperClass(ObjectStoreMapper.class);
      job.setReducerClass(KeyValueStoreReducer.class);
    }

    @Override
    public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    }
  }

  /**
   *
   */
  public static class ObjectStoreMapper extends Mapper<byte[], String, Text, Text> {
    @Override
    public void map(byte[] key, String data, Context context)
      throws IOException, InterruptedException {
      context.write(new Text(data),
                    new Text(Integer.toString(data.length())));
    }
  }

  /**
   *
   */
  public static class KeyValueStoreReducer extends Reducer<Text, Text, byte[], byte[]> {
    public void reduce(Text key, Iterable<Text> values, Context context)
                              throws IOException, InterruptedException  {
      for (Text value : values) {
        context.write(Bytes.toBytes(key.toString()), Bytes.toBytes(value.toString()));
      }
    }
  }

}
