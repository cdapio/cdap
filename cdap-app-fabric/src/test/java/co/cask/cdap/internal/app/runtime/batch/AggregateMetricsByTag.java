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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class AggregateMetricsByTag {
  public static final Logger LOG = LoggerFactory.getLogger(AggregateMetricsByTag.class);
  public static final byte[] BY_TAGS = Bytes.toBytes("byTag");

  /**
   *
   */
  public static class Map
    extends Mapper<byte[], TimeseriesTable.Entry, BytesWritable, LongWritable>
    implements ProgramLifecycle<MapReduceContext> {

    private Metrics metrics;

    @UseDataSet("counters")
    private Table counters;

    private Table countersFromContext;

    public void map(byte[] key, TimeseriesTable.Entry value, Context context) throws IOException, InterruptedException {
      metrics.count("in.map", 1);
      for (byte[] tag : value.getTags()) {
        long val = Bytes.toLong(value.getValue());
        if (55L == val) {
          throw new RuntimeException("Intentional exception: someone on purpose added bad data as input");
        }
        context.write(new BytesWritable(tag), new LongWritable(val));
      }
      counters.increment(new Increment("mapper", "records", 1L));
      countersFromContext.increment(new Increment("mapper", "records", 1L));
      metrics.count("in.map", 2);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      metrics.gauge("in.map.setup", 1);
      LOG.info("in mapper: setup()");
      long mappersCount = counters.incrementAndGet(new Increment("mapper", "count", 1L)).getLong("count", 0);
      LOG.info("mappers started so far: " + mappersCount);
      metrics.gauge("in.map.setup", 1);
    }

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      countersFromContext = context.getDataset("countersFromContext");
    }

    @Override
    public void destroy() {
      // do nothing
    }
  }

  /**
   *
   */
  public static class Reduce
    extends Reducer<BytesWritable, LongWritable, byte[], TimeseriesTable.Entry>
    implements ProgramLifecycle<MapReduceContext> {

    private Metrics metrics;

    @UseDataSet("counters")
    private Table counters;

    private Table countersFromContext;

    public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

      metrics.count("in.reduce", 1);
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
        counters.increment(new Increment("reducer", "records", 1L));
        countersFromContext.increment(new Increment("reducer", "records", 1L));
      }
      byte[] tag = key.copyBytes();
      context.write(tag, new TimeseriesTable.Entry(BY_TAGS, Bytes.toBytes(sum), System.currentTimeMillis(), tag));
      metrics.count("in.reduce", 1);
    }

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
      metrics.gauge("in.reduce.setup", 1);
      LOG.info("in reducer: setup()");
      long reducersCount = counters.incrementAndGet(new Increment("reducer", "count", 1L)).getLong("count", 0);
      LOG.info("reducers started so far: " + reducersCount);
      metrics.gauge("in.reduce.setup", 1);
    }

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      countersFromContext = context.getDataset("countersFromContext");
    }

    @Override
    public void destroy() {
      // do nothing
    }
  }

  static void configureJob(Job job) throws IOException {
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setReducerClass(Reduce.class);
  }
}
