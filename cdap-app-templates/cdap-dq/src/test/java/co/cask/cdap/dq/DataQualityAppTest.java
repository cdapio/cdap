/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.dq;

import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.Formats;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.dq.functions.DiscreteValuesHistogram;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactRange;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.template.etl.batch.source.StreamBatchSource;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;



/**
 * Test for {@link DataQualityApp}.
 */
public class DataQualityAppTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_LIST_TIMESTAMP_VALUE = new TypeToken<ArrayList<TimestampValue>>() { }.getType();
  private static final Type TOKEN_TYPE_DOUBLE = new TypeToken<Double>() { }.getType();
  private static final Type TOKEN_TYPE_MAP_STRING_INTEGER = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final Type TOKEN_TYPE_LIST_AGGREGATION_TYPE_VALUES =
    new TypeToken<List<AggregationTypeValue>>() { }.getType();
  private static final Type TOKEN_TYPE_SET_FIELD_DETAIL = new TypeToken<HashSet<FieldDetail>>() { }.getType();
  private static final Integer WORKFLOW_SCHEDULE_MINUTES = 5;
  private static final String SOURCE_ID = "logStream";

  @Test
  public void test() throws Exception {
    Id.Artifact appArtifact = Id.Artifact.from(Id.Namespace.DEFAULT, "dqArtifact", "1.0");
    addAppArtifact(appArtifact, DataQualityApp.class);

    ArtifactRange artifactRange = new ArtifactRange(appArtifact.getNamespace(), appArtifact.getName(),
                                                    appArtifact.getVersion(), true, new ArtifactVersion("2.0.0"), true);
    Id.Artifact pluginArtifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "source-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, Sets.<ArtifactRange>newHashSet(artifactRange), StreamBatchSource.class,
                      AvroKey.class, GenericRecord.class);

    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "newApp");

    AppRequest<DataQualityApp.ConfigClass> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifact.getName(), appArtifact.getVersion().getVersion(), false),
      new DataQualityApp.ConfigClass());
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    StreamManager streamManager = getStreamManager("logStream");

    String logData1 = "10.10.10.10 - - [01/Feb/2015:06:47:10 +0000] " +
      "\"GET /browse/COOP-DBT-JOB1-238/artifact HTTP/1.1\"" +
      " 301 256 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\"";

    String logData2 = "10.10.12.10 - - [01/Feb/2015:06:47:10 +0000]" +
      " \"GET /browse/COOP-DBT-JOB1-238/artifact HTTP/1.1\"" +
      " 301 256 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\"";

    String logData3 = "10.10.11.10 - - [01/Feb/2015:06:47:10 +0000]" +
      " \"GET /browse/COOP-DBT-JOB1-238/artifact HTTP/1.1\"" +
      " 301 256 \"-\" \"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\"";

    streamManager.send(logData1);
    streamManager.send(logData2);
    streamManager.send(logData3);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    Table logDataStore = (Table) getDataset("dataQuality").get();

    Scanner scanner = logDataStore.scan(null, null);

    DiscreteValuesHistogram discreteValuesHistogramAggregationFunction = new DiscreteValuesHistogram();
    Row row;
    try {
      while ((row = scanner.next()) != null) {
        if (Bytes.toString(row.getRow()).contains("content_length")) {
          Map<byte[], byte[]> columnsMapBytes = row.getColumns();
          byte[] output = columnsMapBytes.get(Bytes.toBytes("DiscreteValuesHistogram"));
          if (output != null) {
            discreteValuesHistogramAggregationFunction.combine(output);
          }
        }
      }
    } finally {
      scanner.close();
    }

    Map<String, Integer> outputMap = discreteValuesHistogramAggregationFunction.retrieveAggregation();
    Map<String, Integer> expectedMap = Maps.newHashMap();
    expectedMap.put("256", 3);
    Assert.assertEquals(expectedMap, outputMap);
  }

  @Test
  public void test2() throws Exception {
    Map<String, Set<String>> testMap = new HashMap<>();
    Set<String> testSet = new HashSet<>();

    testSet.add("Average");
    testMap.put("content_length", testSet);

    DataQualityApp.ConfigClass config = new DataQualityApp.ConfigClass(WORKFLOW_SCHEDULE_MINUTES,
                                                                       SOURCE_ID,
                                                                       "avg",
                                                                       Formats.COMBINED_LOG_FORMAT,
                                                                       null,
                                                                       testMap);
    ApplicationManager applicationManager = deployApplication(DataQualityApp.class, config);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    ServiceManager serviceManager = applicationManager.getServiceManager
      (AggregationsService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    /* Test for aggregationsGetter handler */

    URL url = new URL(serviceManager.getServiceURL(),
                      "v1/sources/logStream/fields/content_length/aggregations/Average/timeseries");
    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    String response = httpResponse.getResponseBodyAsString();

    List<TimestampValue> tsValueListActual = GSON.fromJson(response, TOKEN_TYPE_LIST_TIMESTAMP_VALUE);
    TimestampValue firstTimestampValue = tsValueListActual.get(0);
    Object objActual = firstTimestampValue.getValue();
    String actualJSON = GSON.toJson(objActual);
    Double actualDouble = GSON.fromJson(actualJSON, TOKEN_TYPE_DOUBLE);
    Assert.assertEquals(actualDouble, new Double(256.0));
  }

  @Test
  public void test3() throws Exception {
    Map<String, Set<String>> testMap = new HashMap<>();
    Set<String> testSet = new HashSet<>();

    testSet.add("DiscreteValuesHistogram");
    testMap.put("content_length", testSet);
    testMap.put("status", testSet);
    testMap.put("date", testSet);

    DataQualityApp.ConfigClass config = new DataQualityApp.ConfigClass(WORKFLOW_SCHEDULE_MINUTES,
                                                                       SOURCE_ID,
                                                                       "histogram",
                                                                       Formats.COMBINED_LOG_FORMAT,
                                                                       null,
                                                                       testMap);
    ApplicationManager applicationManager = deployApplication(DataQualityApp.class, config);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    Map<String, Integer> expectedMap = new HashMap<>();
    expectedMap.put("256", 3);

    /* Test for the aggregationsGetter handler */

    ServiceManager serviceManager = applicationManager.getServiceManager
      (AggregationsService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);
    URL url = new URL(serviceManager.getServiceURL(),
                      "v1/sources/logStream/fields/content_length/aggregations/DiscreteValuesHistogram/totals");
    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    String response = httpResponse.getResponseBodyAsString();

    Map<String, Integer> histogramMap = GSON.fromJson(response, TOKEN_TYPE_MAP_STRING_INTEGER);
    Assert.assertEquals(expectedMap, histogramMap);

    /* Test for the fieldsGetter handler */

    url = new URL(serviceManager.getServiceURL(), "v1/sources/logStream/fields");
    httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    response = httpResponse.getResponseBodyAsString();

    Set<FieldDetail> outputSet = GSON.fromJson(response, TOKEN_TYPE_SET_FIELD_DETAIL);
    Set<FieldDetail> expectedSet = new HashSet<>();
    AggregationTypeValue aggregationTypeValue = new AggregationTypeValue("DiscreteValuesHistogram", true);
    Set<AggregationTypeValue> aggregationTypeValuesList = Sets.newHashSet(aggregationTypeValue);
    expectedSet.add(new FieldDetail("content_length", aggregationTypeValuesList));
    expectedSet.add(new FieldDetail("date", aggregationTypeValuesList));
    expectedSet.add(new FieldDetail("status", aggregationTypeValuesList));
    Assert.assertEquals(expectedSet, outputSet);

    /* Test for the aggregationTypesGetter handler */

    url = new URL(serviceManager.getServiceURL(), "v1/sources/logStream/fields/content_length");
    httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    response = httpResponse.getResponseBodyAsString();

    List<AggregationTypeValue> expectedAggregationTypeValuesList = new ArrayList<>();
    List<AggregationTypeValue> outputAggregationTypeValuesList =
      GSON.fromJson(response, TOKEN_TYPE_LIST_AGGREGATION_TYPE_VALUES);
    expectedAggregationTypeValuesList.add(new AggregationTypeValue("DiscreteValuesHistogram", true));
    Assert.assertEquals(expectedAggregationTypeValuesList, outputAggregationTypeValuesList);
  }
}

