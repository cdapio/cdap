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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * App which copies data from a stream to KVTable using a MapReduce program.
 */
public class StreamWithMRApp extends AbstractApplication {

  public static final String APP = "StreamWithMRApp";
  public static final String STREAM = "inputStream";
  public static final String MAPREDUCE = "MRCopy";
  public static final String KVTABLE = "kvtable";

  @Override
  public void configure() {
    setName(APP);
    setDescription("Copy Data from Stream to KVTable.");
    addStream(STREAM);
    createDataset(KVTABLE, KeyValueTable.class);
    addMapReduce(new CopyMapReduce());
  }

  public static class CopyMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName(MAPREDUCE);
    }

    @Override
    public void initialize() {
      MapReduceContext context = getContext();
      context.addInput(Input.ofStream(STREAM));
      context.addOutput(Output.ofDataset(KVTABLE));
      Job hadoopJob = context.getHadoopJob();
      hadoopJob.setMapperClass(IdentityMapper.class);
      hadoopJob.setNumReduceTasks(0);
    }

    public static class IdentityMapper extends Mapper<LongWritable, String, byte[], byte[]> {

      public void map(LongWritable ts, String event, Context context) throws IOException, InterruptedException {
        context.write(Bytes.toBytes(event), Bytes.toBytes(event));
      }
    }
  }
}
