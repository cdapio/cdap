package com.continuuity.internal.app.runtime.batch.hadoop;

import com.continuuity.api.data.dataset.TimeseriesTable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class AggregateMetricsByTag {
  public static final byte[] BY_TAGS = com.continuuity.common.utils.Bytes.toBytes("byTag");

  public static class Map extends Mapper<byte[], TimeseriesTable.Entry, BytesWritable, LongWritable> {
    public void map(byte[] key, TimeseriesTable.Entry value, Context context) throws IOException, InterruptedException {
      for (byte[] tag : value.getTags()) {
        context.write(new BytesWritable(tag), new LongWritable(com.continuuity.common.utils.Bytes.toLong(value.getValue())));
      }
    }
  }

  public static class Reduce extends Reducer<BytesWritable, LongWritable, byte[], TimeseriesTable.Entry> {
    public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      byte[] tag = key.copyBytes();
      context.write(tag, new TimeseriesTable.Entry(BY_TAGS, com.continuuity.common.utils.Bytes.toBytes(sum), System.currentTimeMillis(), tag));
    }
  }

  static void configureJob(Job job) throws IOException {
    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setReducerClass(Reduce.class);
  }
}
