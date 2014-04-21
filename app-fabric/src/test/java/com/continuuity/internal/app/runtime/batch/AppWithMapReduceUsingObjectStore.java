package com.continuuity.internal.app.runtime.batch;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.google.common.base.Throwables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 */
public class AppWithMapReduceUsingObjectStore implements Application {
  @Override
  public ApplicationSpecification configure() {
    try {
      return ApplicationSpecification.Builder.with()
        .setName("AppWithMapReduceObjectStore")
        .setDescription("Application with MapReduce job using objectstore as dataset")
        .noStream()
        .withDataSets()
          .add(new ObjectStore<String>("keys", String.class))
          .add(new KeyValueTable("count"))
        .noFlow()
        .noProcedure()
        .withMapReduce()
          .add(new ComputeCounts())
        .noWorkflow()
        .build();
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }

  /**
   *
   */
  public static final class ComputeCounts implements MapReduce {
    @Override
    public MapReduceSpecification configure() {
      return MapReduceSpecification.Builder.with()
        .setName("ComputeCounts")
        .setDescription("Use Objectstore dataset as input job")
        .useInputDataSet("keys")
        .useOutputDataSet("count")
        .build();
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      Job job = (Job) context.getHadoopJob();
      job.setMapperClass(ObjectStoreMapper.class);
      job.setReducerClass(KeyValueStoreReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
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
