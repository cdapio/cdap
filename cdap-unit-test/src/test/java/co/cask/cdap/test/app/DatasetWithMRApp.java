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

package co.cask.cdap.test.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * App which copies data from one KVTable to another using a MapReduce program.
 */
public class DatasetWithMRApp extends AbstractApplication {

  public static final String INPUT_KEY = "input";
  public static final String OUTPUT_KEY = "output";
  public static final String MAPREDUCE_PROGRAM = "copymr";

  @Override
  public void configure() {
    setName("DatasetWithMRApp");
    setDescription("Copy Data from one KVTable Dataset to another");
    addMapReduce(new CopyMapReduce());
  }

  public static class CopyMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      setName(MAPREDUCE_PROGRAM);
    }

    @Override
    public void beforeSubmit(MapReduceContext context) {
      context.setInput(context.getRuntimeArguments().get(INPUT_KEY));
      context.setOutput(context.getRuntimeArguments().get(OUTPUT_KEY));
      Job hadoopJob = context.getHadoopJob();
      hadoopJob.setMapperClass(IdentityMapper.class);
      hadoopJob.setNumReduceTasks(0);
    }

    public static class IdentityMapper extends Mapper<byte[], byte[], byte[], byte[]> {

      public void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
      }
    }
  }
}
