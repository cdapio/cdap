/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.data2.metrics;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.http.HttpMethod;
import co.cask.cdap.common.http.HttpRequest;
import co.cask.cdap.common.http.HttpRequests;
import co.cask.cdap.common.http.HttpResponse;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.Namespace;
import co.cask.cdap.data2.datafabric.DefaultDatasetNamespace;
import co.cask.cdap.data2.dataset2.DatasetNamespace;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Collects HBase-based dataset's metrics from HBase.
 */
public final class HBaseDatasetMetricsCollector {
  public static final String HBASE_REGIONSERVER_INFO_PORT = "hbase.regionserver.info.port";
  private static final Gson GSON = new Gson();

  private HBaseDatasetMetricsCollector() {}

  // todo
  public static void collect(MetricsCollectionService metricsService, Configuration hConf, CConfiguration cConf)
    throws IOException {

    // todo : describe two ways of doing collection: using table names or collection all by namespace (chosen)

    HBaseAdmin admin = new HBaseAdmin(hConf);
    Map<String, Integer> datasetSize = Maps.newHashMap();
    ClusterStatus clusterStatus = admin.getClusterStatus();
    for (ServerName serverName : clusterStatus.getServers()) {
      HttpResponse response = HttpRequests.execute(
        HttpRequest.builder(HttpMethod.GET,
                            new URL("http://" + serverName.getHostname() +
                                      ":" + hConf.get(HBASE_REGIONSERVER_INFO_PORT) +
                                      "/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions")).build());

      String responseBody = response.getResponseBodyAsString();

      JsonObject jsonObject = GSON.fromJson(responseBody, JsonObject.class);
      JsonArray beans = jsonObject.getAsJsonArray("beans");
      JsonElement jsonElement = beans.get(0);
      Map<String, String> fields = GSON.fromJson(jsonElement, new TypeToken<Map<String, String>>() { }.getType());
      String hbaseNamespace = cConf.get(Constants.Dataset.HBASE_TABLE_NAMESPACE);
      DatasetNamespace userNamespace = new DefaultDatasetNamespace(cConf, Namespace.USER);

      Pattern pattern =
        Pattern.compile("namespace_" + Pattern.quote(hbaseNamespace) + "_table_(.*)_region_.{32}_metric_(.*)");

      for (Map.Entry<String, String> entry : fields.entrySet()) {
        String key = entry.getKey();
        Matcher matcher = pattern.matcher(key);
        if (matcher.matches() && matcher.groupCount() >= 2) {
          String tableName = matcher.group(1);
          tableName = userNamespace.fromNamespaced(tableName);
          boolean tableInUserNamespace = tableName != null;
          if (tableInUserNamespace) {
            Integer aggregatedSize = datasetSize.get(tableName);
            if (aggregatedSize == null) {
              aggregatedSize = 0;
            }
            String metricName = matcher.group(2);
            // todo: do we include memstore size? or only disk size?
            if ("storeFileSize".equals(metricName) || "memStoreSize".equals(metricName)) {
              aggregatedSize += Integer.parseInt(entry.getValue());
            }
            datasetSize.put(tableName, aggregatedSize);
          }
        }
      }
    }

    MetricsCollector collector =
      metricsService.getCollector(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");
    for (Map.Entry<String, Integer> sizeEntry : datasetSize.entrySet()) {
      // todo: do we want to report dataset metric right here?
      //       or store in metrics system and allow dataset admin to report this metric? Can HBase be exception?
      //       NOTE: for composite datasets the dataset admin still has to aggregate and report these metrics
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      collector.gauge("dataset.store.bytes", sizeEntry.getValue(), sizeEntry.getKey());
    }
  }
}
