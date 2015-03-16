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

package co.cask.cdap.templates.etl.core;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import co.cask.cdap.templates.etl.lib.sinks.batch.KVTableBatchSink;
import co.cask.cdap.templates.etl.lib.sources.batch.BatchKafkaSource;
import com.google.common.collect.Lists;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class BatchAdapterDriver extends AbstractMapReduce {

  @Override
  public void configure() {
    setName("BatchAdapterDriver");
    setDescription("Driving Batch Adapters");
    useDatasets("KVTableBatchSink");
    useDatasets("KVTableBatchSource");
  }

  @Override
  public void beforeSubmit(MapReduceContext context) {
    Job job = context.getHadoopJob();
    //TODO: Still required?
    job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");

    //Prepare Job Phase
    prepareJob(context);

    job.setMapperClass(MapperDriver.class);
    job.setNumReduceTasks(0);
  }

  private void prepareJob(MapReduceContext context) {
    BatchSource batchSource = new BatchKafkaSource();
    BatchSink batchSink = new KVTableBatchSink();
    batchSource.prepareJob(context);
    batchSink.prepareJob(context);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    BatchSource batchSource = new BatchKafkaSource();
    BatchSink batchSink = new KVTableBatchSink();
    batchSource.onFinish(succeeded, context);
    batchSink.onFinish(succeeded, context);
  }

  public static class MapperDriver extends Mapper implements ProgramLifecycle<MapReduceContext> {

    private MapReduceContext mrContext;
    private BatchSource batchSource;
    private BatchSink batchSink;

    @Override
    public void initialize(MapReduceContext mapReduceContext) throws Exception {
      this.mrContext = mapReduceContext;
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      batchSource = new BatchKafkaSource();
      batchSink = new KVTableBatchSink();
      batchSource.initialize(mrContext);
      batchSink.initialize(mrContext);
    }

    @Override
    public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
      final List<Object> sourceList = Lists.newArrayList();
      batchSource.read(key, value, new Emitter() {
        @Override
        public void emit(Object obj) {
          sourceList.add(obj);
        }
      });

      for (Object obj : sourceList) {
        batchSink.write(context, obj);
      }
    }

    @Override
    public void destroy() {
      batchSource.destroy();
      batchSink.destroy();
    }
  }

}
