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
package co.cask.cdap.examples.purchase;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.Resources;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.api.metrics.Metrics;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * MapReduce job that reads purchases from the purchases DataSet and creates a purchase history for every user
 */
public class PurchaseHistoryBuilder extends AbstractMapReduce {
  public static final String MAPPER_MEMORY_MB = "mapper.memory.mb";
  public static final String REDUCER_MEMORY_MB = "reducer.memory.mb";

  @Override
  public void configure() {
    setDescription("Purchase History Builder MapReduce job");
    useDatasets("frequentCustomers");
    setInputDataset("purchases");
    setOutputDataset("history");
    setMapperResources(new Resources(1024));
    setReducerResources(new Resources(1024));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    Job job = context.getHadoopJob();
    job.setMapperClass(PurchaseMapper.class);
    job.setReducerClass(PerUserReducer.class);

    // override default memory usage if the corresponding runtime arguments are set.
    Map<String, String> runtimeArgs = context.getRuntimeArguments();
    String mapperMemoryMBStr = runtimeArgs.get(MAPPER_MEMORY_MB);
    if (mapperMemoryMBStr != null) {
      context.setMapperResources(new Resources(Integer.parseInt(mapperMemoryMBStr)));
    }
    String reducerMemoryMBStr = runtimeArgs.get(REDUCER_MEMORY_MB);
    if (reducerMemoryMBStr != null) {
      context.setReducerResources(new Resources(Integer.parseInt(reducerMemoryMBStr)));
    }
  }

  /**
   * Mapper class to emit user and corresponding purchase information
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
   * Reducer class to aggregate all purchases per user
   */
  public static class PerUserReducer extends Reducer<Text, Text, String, PurchaseHistory>
    implements ProgramLifecycle<MapReduceContext> {

    @UseDataSet("frequentCustomers")
    private KeyValueTable frequentCustomers;

    private Metrics reduceMetrics;

    private URL userProfileServiceURL;

    private static final int RARE_PURCHASE_COUNT = 1;
    private static final int FREQUENT_PURCHASE_COUNT = 10;

    private static final Logger LOG = LoggerFactory.getLogger(PerUserReducer.class);

    @Override
    public void initialize(MapReduceContext context) throws Exception {
      userProfileServiceURL = context.getServiceURL(UserProfileServiceHandler.SERVICE_NAME);
    }

    public void reduce(Text customer, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
      UserProfile userProfile = null;
      try {
        URL url = new URL(userProfileServiceURL,
                                        UserProfileServiceHandler.USER_ENDPOINT
                                          + "/" + customer.toString());

        HttpRequest request = HttpRequest.get(url).build();
        HttpResponse response = HttpRequests.execute(request);
        if (response.getResponseCode() != HttpURLConnection.HTTP_NO_CONTENT) {
          userProfile = new Gson().fromJson(new String(response.getResponseBody()
            , Charsets.UTF_8), UserProfile.class);
        }
      } catch (Exception e) {
        LOG.warn("Error accessing user profile: {}", e.getCause());
      }

      PurchaseHistory purchases = new PurchaseHistory(customer.toString(), userProfile);
      int numPurchases = 0;
      for (Text val : values) {
        purchases.add(new Gson().fromJson(val.toString(), Purchase.class));
        numPurchases++;
      }
      if (numPurchases == RARE_PURCHASE_COUNT) {
        reduceMetrics.count("customers.rare", 1);
      } else if (numPurchases > FREQUENT_PURCHASE_COUNT) {
        reduceMetrics.count("customers.frequent", 1);
        frequentCustomers.write(customer.toString(), String.valueOf(numPurchases));
      }

      context.write(customer.toString(), purchases);
    }

    @Override
    public void destroy() {
      // no-op
    }
  }
}
