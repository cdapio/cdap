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

package com.continuuity.testsuite.purchaseanalytics;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.ObjectStore;
import com.continuuity.api.mapreduce.AbstractMapReduce;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Customer;
import com.continuuity.testsuite.purchaseanalytics.datamodel.Purchase;
import com.continuuity.testsuite.purchaseanalytics.datamodel.PurchaseHistory;
import com.continuuity.testsuite.purchaseanalytics.datamodel.PurchaseStat;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Map reduce job that aggregates total spending and average spending for each customers by looking up at their purchase
 * history if any. test performing lookup from map reduce job.
 */
public class PurchaseStatsBuilder extends AbstractMapReduce {
  @Override
  public MapReduceSpecification configure() {
    return MapReduceSpecification.Builder.with()
      .setName("PurchaseStatsBuilder")
      .setDescription("Aggregates total and average spending per customer.")
      .useInputDataSet("customers")
      .useOutputDataSet("purchaseStats")
      .build();
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = (Job) context.getHadoopJob();
    job.setMapperClass(PurchaseStatsMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(PurchaseStatsReducer.class);
  }

  public static class PurchaseStatsMapper extends Mapper<byte[], Customer, Text, Text> {

    @UseDataSet("history")
    ObjectStore<PurchaseHistory> purchaseHistory;

    @Override
    public void map(byte[] key, Customer customer, Context context)
      throws IOException, InterruptedException {

      try {
      // Get purchase history by looking up dataset
      PurchaseHistory aPurchaseHistory = purchaseHistory.read(Bytes.toBytes(customer.getName()));

      context.write(new Text(String.valueOf(customer.getCustomerId())),
                    new Text(new Gson().toJson(aPurchaseHistory)));
      }
      catch (OperationException ex) {
       ex.printStackTrace(); // TODO: add better logging.
      }
    }
  }

  public static class PurchaseStatsReducer extends Reducer<Text, Text, byte[], PurchaseStat> {

    public void reduce(Text customerId, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      long totalSpent = 0;
      double averageSpent = 0;
      int numPurchases = 0;
      long custId = Long.parseLong(customerId.toString());

      // Sum up total Spent.
      for (Text val : values) {
        PurchaseHistory purchaseHistory = new Gson().fromJson(val.toString(), PurchaseHistory.class);

        for (Purchase purchase : purchaseHistory.getPurchases()) {
          totalSpent += purchase.getPrice() * purchase.getQuantity();
          numPurchases++;
        }
      }

      averageSpent = (double) totalSpent / (double) numPurchases;

        PurchaseStat purchaseStat = new PurchaseStat(custId, averageSpent, totalSpent);
      context.write(Bytes.toBytes(custId), purchaseStat);
    }
  }
}
