/**
 * Copyright 2013-2014 Continuuity, Inc.
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
package com.continuuity.examples.purchase;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.dataset.lib.KeyValueTable;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.api.metrics.Metrics;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MapReduce job that reads purchases from the purchases DataSet and creates a purchase history for every user.
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

  /**
   * Mapper class.
   */
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

  /**
   * Reducer class.
   */
  public static class PerUserReducer extends Reducer<Text, Text, String, PurchaseHistory> {
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

      context.write(customer.toString(), purchases);
    }
  }
}
