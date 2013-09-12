package com.continuuity.examples.resourcespammer;

import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map reduce job that reads Purchases from Object store and creates purchase history for every user.
 */
public class MapredSpammer extends AbstractMapReduce {


  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("MapredSpammer")
      .setDescription("Map Reduce job that just spins")
      .useInputDataSet("input")
      .useOutputDataSet("output")
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(SpamMapper.class);
    job.setMapOutputKeyClass(Integer.class);
    job.setMapOutputValueClass(Integer.class);
    job.setReducerClass(SpamReducer.class);
  }

  public class SpamMapper extends Mapper<byte[], byte[], Integer, Integer>{

    private final Spammer spammer = new Spammer(1);

    public void map(byte[] key, byte[] value, Context context) throws IOException, InterruptedException {
      spammer.spamFor(1000 * 1000 * 1000);
      context.write(1, 1);
    }
  }

  public static class SpamReducer extends Reducer<Integer, Integer, byte[], byte[]> {

    private final Spammer spammer = new Spammer(1);

    public void reduce(Integer key, Iterable<Integer> values, Context context)
      throws IOException, InterruptedException {
      spammer.spamFor(1000 * 1000 * 1000);
      context.write(Bytes.toBytes(key), Bytes.toBytes(key));
    }
  }
}
