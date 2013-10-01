package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
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
  public static class Map extends Mapper<byte[], TimeseriesTable.Entry, BytesWritable, LongWritable> {
    @UseDataSet("counters")
    private Table counters;

    public void map(byte[] key, TimeseriesTable.Entry value, Context context) throws IOException, InterruptedException {
      for (byte[] tag : value.getTags()) {
        long val = Bytes.toLong(value.getValue());
        if (55L == val) {
          throw new RuntimeException("Intentional exception: someone on purpose added bad data as input");
        }
        context.write(new BytesWritable(tag), new LongWritable(val));
      }
      counters.increment(new Increment("mapper", "records", 1L));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      LOG.info("in mapper: setup()");
      long mappersCount = counters.increment(new Increment("mapper", "count", 1L)).getLong("count", 0);
      LOG.info("mappers started so far: " + mappersCount);
    }
  }

  /**
   *
   */
  public static class Reduce extends Reducer<BytesWritable, LongWritable, byte[], TimeseriesTable.Entry> {
    @UseDataSet("counters")
    private Table counters;

    public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
        counters.increment(new Increment("reducer", "records", 1L));
      }
      byte[] tag = key.copyBytes();
      context.write(tag, new TimeseriesTable.Entry(BY_TAGS, Bytes.toBytes(sum), System.currentTimeMillis(), tag));
    }

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
      LOG.info("in reducer: setup()");
      long reducersCount = counters.increment(new Increment("reducer", "count", 1L)).getLong("count", 0);
      LOG.info("reducers started so far: " + reducersCount);
    }
  }

  static void configureJob(Job job) throws IOException {
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setReducerClass(Reduce.class);
  }
}
