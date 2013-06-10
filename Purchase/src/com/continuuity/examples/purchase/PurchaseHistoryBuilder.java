package com.continuuity.examples.purchase;

import com.continuuity.api.batch.MapReduce;
import com.continuuity.api.batch.MapReduceContext;
import com.continuuity.api.batch.MapReduceSpecification;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 */
public class PurchaseHistoryBuilder implements MapReduce  {


  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with().
       setName("PurchaseHistoryBuilder").
       setDescription("Purchase History Builder Map Reduce job").useInputDataSet("purchases").
       useOutputDataSet("history").
       build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = (Job) context.getHadoopJob();
    job.setMapperClass(PurchaseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(PerUserReducer.class);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
  }

  public static class PurchaseMapper extends Mapper<byte[], Purchase, Text, Text> {
    @Override
    public void map(byte[] key, Purchase purchase, Context context)
      throws IOException, InterruptedException {
      String user = purchase.getCustomer();
      context.write(new Text(user), new Text(new Gson().toJson(purchase)));
    }
  }

  public static class PerUserReducer extends Reducer<Text, Text, String, ArrayList<Purchase>> {

    public void reduce(Text user, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      ArrayList<Purchase> purchases = Lists.newArrayList();
      for (Text val : values) {
        purchases.add(new Gson().fromJson(val.toString(), Purchase.class));
      }
      context.write(user.toString(), purchases);
    }
  }

}
