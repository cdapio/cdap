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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Collects HBase-based dataset's metrics from HBase.
 */
public class HBaseDatasetStatsReporter extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseDatasetStatsReporter.class);

  private static final String HBASE_REGIONSERVER_INFO_PORT = "hbase.regionserver.info.port";
  private static final Gson GSON = new Gson();

  private volatile ScheduledExecutorService executor;
  private final int reportIntervalInSec;

  private final MetricsCollectionService metricsService;
  private final Configuration hConf;

  public HBaseDatasetStatsReporter(MetricsCollectionService metricsService, Configuration hConf, CConfiguration conf) {
    this.metricsService = metricsService;
    this.hConf = hConf;
    this.reportIntervalInSec = conf.getInt(Constants.Metrics.Dataset.HBASE_STATS_REPORT_INTERVAL_IN_SEC);
  }

  @Override
  protected void shutDown() throws Exception {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    reportHBaseStats();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, reportIntervalInSec, TimeUnit.SECONDS);
  }

  @Override
  protected final ScheduledExecutorService executor() {
    executor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("HBaseDatasetStatsReporter-scheduler"));
    return executor;
  }

  private void reportHBaseStats() throws IOException {
    Map<String, TableStats> tableStats = getTableStats(hConf);
    if (tableStats.size() > 0) {
      report(tableStats);
    }
  }

  @VisibleForTesting
  static Map<String, TableStats> getTableStats(Configuration hConf) throws IOException {
    // The idea is to walk thru live region servers, collect table region stats and aggregate them towards table total
    // metrics.

    HBaseAdmin admin = new HBaseAdmin(hConf);
    Map<String, TableStats> datasetStat = Maps.newHashMap();
    ClusterStatus clusterStatus = admin.getClusterStatus();

    for (ServerName serverName : clusterStatus.getServers()) {
      String url = String.format("http://%s:%s/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions",
                                 serverName.getHostname(), hConf.get(HBASE_REGIONSERVER_INFO_PORT));
      HttpResponse response = HttpRequests.execute(HttpRequest.builder(HttpMethod.GET, new URL(url)).build());

      if (HttpURLConnection.HTTP_OK != response.getResponseCode()) {
        // We want to skip this reporting round since aggregated stats could be crewed in this case
        LOG.error(String.format("Failed to get metrics from server %s: got response code %d." +
                                  " Skipping this reporting iteration", url, response.getResponseCode()));
        return Maps.newHashMap();
      }

      String responseBody = response.getResponseBodyAsString();

      Map<String, String> fields;
      try {
        JsonObject jsonObject = GSON.fromJson(responseBody, JsonObject.class);
        JsonArray beans = jsonObject.getAsJsonArray("beans");
        JsonElement jsonElement = beans.get(0);
        fields = GSON.fromJson(jsonElement, new TypeToken<Map<String, String>>() { }.getType());
      } catch (Exception e) {
        // We want to skip this reporting round so that latest successfully reported values are last ones reported
        LOG.error(String.format("Failed to get metrics from server %s: parsing response failed." +
                                  " Skipping this reporting iteration", url), e);
        return Maps.newHashMap();
      }

      Pattern pattern = Pattern.compile("namespace_.*_table_(.*)_region_.{32}_metric_(.*)");

      for (Map.Entry<String, String> entry : fields.entrySet()) {
        String key = entry.getKey();
        Matcher matcher = pattern.matcher(key);
        if (matcher.matches() && matcher.groupCount() >= 2) {
          String tableName = matcher.group(1);
          TableStats stat = datasetStat.get(tableName);
          if (stat == null) {
            stat = new TableStats();
            datasetStat.put(tableName, stat);
          }
          String metricName = matcher.group(2);
          if ("storeFileSize".equals(metricName)) {
            stat.storeFileSize += Integer.parseInt(entry.getValue());
          } else if ("memStoreSize".equals(metricName)) {
            stat.memStoreSize += Integer.parseInt(entry.getValue());
          }
        }
      }
    }
    return datasetStat;
  }

  private void report(Map<String, TableStats> datasetStat) {
    // we use "0" as runId: it is required by metrics system to provide something at this point
    MetricsCollector collector =
      metricsService.getCollector(MetricsScope.REACTOR, Constants.Metrics.DATASET_CONTEXT, "0");
    for (Map.Entry<String, TableStats> statEntry : datasetStat.entrySet()) {
      // table name = dataset name for metrics
      String datasetName = statEntry.getKey();
      // legacy format: dataset name is in the tag. See DatasetInstantiator for more details
      collector.gauge("dataset.store.bytes", statEntry.getValue().getTotalSize(), datasetName);
    }
  }

  @VisibleForTesting
  static final class TableStats {
    private int storeFileSize = 0;
    private int memStoreSize = 0;

    public int getTotalSize() {
      // both memstore and size on fs contribute to size of the dataset, otherwise user will be confused with zeroes
      // in dataset size even after something was written...
      return storeFileSize + memStoreSize;
    }
  }
}
