/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.metrics.Metrics;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MapReduce job that reads purchases from the purchases dataset and creates a purchase history for every user.
 */
public class PurchaseHistoryBuilder extends AbstractMapReduce {


  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("PurchaseHistoryBuilder")
      .setDescription("Purchase History Builder MapReduce job")
      .useDataSet("frequentCustomers")
      .useInputDataSet("purchases")
      .useOutputDataSet("history")
      .setMapperMemoryMB(512)
      .setReducerMemoryMB(1024)
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = (Job) context.getHadoopJob();
    job.setMapperClass(PurchaseMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(PerUserReducer.class);
  }

  public static class PurchaseMapper extends Mapper<byte[], Purchase, Text, Text> {
    private Metrics mapMetrics;

    @Override
    public void map(byte[] key, Purchase purchase, Context context)
      throws IOException, InterruptedException {
      String user = purchase.getCustomer();
      if (purchase.getPrice() > 100000) {
        mapMetrics.count("purchases.large", 1);
      }
      context.write(new Text(user), new Text(new Gson().toJson(purchase)));
    }
  }

  public static class PerUserReducer extends Reducer<Text, Text, byte[], PurchaseHistory> {
    @UseDataSet("frequentCustomers")
    private KeyValueTable frequentCustomers;
    private Metrics reduceMetrics;

    public void reduce(Text customer, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      PurchaseHistory purchases = new PurchaseHistory(customer.toString());
      int numPurchases = 0;
      for (Text val : values) {
        purchases.add(new Gson().fromJson(val.toString(), Purchase.class));
        numPurchases++;
      }
      if (numPurchases == 1) {
        reduceMetrics.count("customers.rare", 1);
      } else if (numPurchases > 10) {
        reduceMetrics.count("customers.frequent", 1);
        frequentCustomers.write(customer.toString(), String.valueOf(numPurchases));
      }

      context.write(Bytes.toBytes(customer.toString()), purchases);
    }
  }
}
