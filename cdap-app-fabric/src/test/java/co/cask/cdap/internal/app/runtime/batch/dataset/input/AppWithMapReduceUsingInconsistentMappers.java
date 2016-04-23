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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * App used to test that a MapReduce using multiple mappers must be of same output key/value types.
 */
public class AppWithMapReduceUsingInconsistentMappers extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithMapReduceUsingInconsistentMappers");
    setDescription("Application with MapReduce jobs, to test Mapper type checking");
    addMapReduce(new MapReduceWithConsistentMapperTypes());
    addMapReduce(new MapReduceWithInconsistentMapperTypes());
    addMapReduce(new MapReduceWithInconsistentMapperTypes2());
    // the types of the datasets do not matter, because we never actually write or read to/from them
    createDataset("input1", KeyValueTable.class);
    createDataset("input2", KeyValueTable.class);
    createDataset("output", KeyValueTable.class);
  }

  /**
   * Performs no data operations, but simply runs to test mapper output type checking.
   */
  private abstract static class BaseMapReduce extends AbstractMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      // the inputs will be set in child classes

      context.addOutput(Output.ofDataset("output"));

      Job job = context.getHadoopJob();
      job.setReducerClass(SomeReducer.class);
    }
  }

  /**
   * MapReduce job that has two mapper classes, both with the same output types.
   */
  public static final class MapReduceWithConsistentMapperTypes extends BaseMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      context.addInput(Input.ofDataset("input1"), OriginalMapper.class);
      context.addInput(Input.ofDataset("input2"), ConsistentMapper.class);
      super.beforeSubmit(context);
    }
  }

  /**
   * MapReduce job that has two mapper classes, each with different output types, both set through the CDAP APIs.
   */
  public static final class MapReduceWithInconsistentMapperTypes extends BaseMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      context.addInput(Input.ofDataset("input1"), OriginalMapper.class);
      context.addInput(Input.ofDataset("input2"), InconsistentMapper.class);

      Job job = context.getHadoopJob();
      // none of the inputs default to the job-defined mapper, so an inconsistent mapper defined here gives no issue
      job.setMapperClass(InconsistentMapper.class);
      super.beforeSubmit(context);
    }
  }

  /**
   * MapReduce job that has two mapper classes, each with different output types (one set through CDAP APIs and one set
   * directly on the job).
   */
  public static final class MapReduceWithInconsistentMapperTypes2 extends BaseMapReduce {

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      context.addInput(Input.ofDataset("input1"), OriginalMapper.class);
      context.addInput(Input.ofDataset("input2"));


      Job job = context.getHadoopJob();
      job.setMapperClass(InconsistentMapper.class);
      super.beforeSubmit(context);
    }
  }

  /**
   * Simple Mapper class.
   */
  public static class OriginalMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  }

  /**
   * Mapper class that has output types, same as {@link OriginalMapper}.
   */
  public static class ConsistentMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  }

  /**
   * Mapper class that has output types being different than {@link OriginalMapper}.
   */
  public static class InconsistentMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

  }

  /**
   * Simple Reducer class.
   */
  public static class SomeReducer extends Reducer<LongWritable, Text, String, String> {

  }

}
