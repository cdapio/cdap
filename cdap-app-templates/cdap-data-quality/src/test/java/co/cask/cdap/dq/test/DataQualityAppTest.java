/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.dq.test;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.dq.AggregationTypeValue;
import co.cask.cdap.dq.DataQualityApp;
import co.cask.cdap.dq.DataQualityService;
import co.cask.cdap.dq.DataQualitySource;
import co.cask.cdap.dq.FieldDetail;
import co.cask.cdap.dq.TimestampValue;
import co.cask.cdap.dq.functions.DiscreteValuesHistogram;
import co.cask.cdap.dq.testclasses.StreamBatchSource;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.artifact.ArtifactSummary;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
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
import javax.annotation.Nullable;

/**
 * Test for {@link DataQualityApp}.
 */
public class DataQualityAppTest extends TestBase {
  private static final Gson GSON = new Gson();
  private static final Type TOKEN_TYPE_LIST_TIMESTAMP_VALUE = new TypeToken<ArrayList<TimestampValue>>() { }.getType();
  private static final Type TOKEN_TYPE_MAP_STRING_INTEGER = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final Type TOKEN_TYPE_LIST_AGGREGATION_TYPE_VALUES =
    new TypeToken<List<AggregationTypeValue>>() { }.getType();
  private static final Type TOKEN_TYPE_SET_FIELD_DETAIL = new TypeToken<HashSet<FieldDetail>>() { }.getType();
  private static final Integer WORKFLOW_SCHEDULE_MINUTES = 5;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  private static ArtifactId appArtifact;
  private static boolean sentData = false;

  @BeforeClass
  public static void setup() throws Exception {
    appArtifact = NamespaceId.DEFAULT.artifact("dqArtifact", "1.0");
    addAppArtifact(appArtifact, DataQualityApp.class, BatchSource.class.getPackage().getName(),
                   PipelineConfigurer.class.getPackage().getName());

    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("source-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, appArtifact, StreamBatchSource.class, AvroKey.class, GenericRecord.class);
  }

  @Before
  public void beforeTest() throws Exception {
    if (sentData) {
      return;
    }
    sentData = true;

    // getStreamManager is not available from a static context (from a beforeClass)
    StreamManager streamManager = getStreamManager("logStream");
    streamManager.createStream();
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
  }

  @Test(expected = IllegalStateException.class)
  public void testInvalidConfig() throws Exception {
    ApplicationId appId = NamespaceId.DEFAULT.app("badApp");
    Map<String, Set<String>> testMap = new HashMap<>();
    // Empty aggregation set - should throw an exception while creating an application
    testMap.put("content_length", new HashSet<String>());

    DataQualityApp.DataQualityConfig config = new DataQualityApp.DataQualityConfig(
      50, getStreamSource(), "avg", testMap);

    AppRequest<DataQualityApp.DataQualityConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifact.getArtifact(), appArtifact.getVersion()), config);
    deployApplication(appId, appRequest);
  }

  @Test
  public void testDefaultConfig() throws Exception {
    Map<String, Set<String>> testMap = new HashMap<>();
    Set<String> testSet = new HashSet<>();

    testSet.add("DiscreteValuesHistogram");
    testMap.put("content_length", testSet);

    DataQualityApp.DataQualityConfig config = new DataQualityApp.DataQualityConfig(
      WORKFLOW_SCHEDULE_MINUTES, getStreamSource(), "dataQuality", testMap);
    ApplicationId appId = NamespaceId.DEFAULT.app("newApp");
    AppRequest<DataQualityApp.DataQualityConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifact.getArtifact(), appArtifact.getVersion()), config);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    Table logDataStore = (Table) getDataset("dataQuality").get();

    DiscreteValuesHistogram discreteValuesHistogramAggregationFunction = new DiscreteValuesHistogram();
    Row row;
    try (Scanner scanner = logDataStore.scan(null, null)) {
      while ((row = scanner.next()) != null) {
        if (Bytes.toString(row.getRow()).contains("content_length")) {
          Map<byte[], byte[]> columnsMapBytes = row.getColumns();
          byte[] output = columnsMapBytes.get(Bytes.toBytes("DiscreteValuesHistogram"));
          if (output != null) {
            discreteValuesHistogramAggregationFunction.combine(output);
          }
        }
      }
    }

    Map<String, Integer> outputMap = discreteValuesHistogramAggregationFunction.retrieveAggregation();
    Map<String, Integer> expectedMap = Maps.newHashMap();
    expectedMap.put("256", 3);
    Assert.assertEquals(expectedMap, outputMap);
  }

  @Test
  public void testMeanContentLength() throws Exception {
    Map<String, Set<String>> testMap = new HashMap<>();
    Set<String> testSet = new HashSet<>();

    testSet.add("Mean");
    testMap.put("content_length", testSet);

    DataQualityApp.DataQualityConfig config = new DataQualityApp.DataQualityConfig(
      WORKFLOW_SCHEDULE_MINUTES, getStreamSource(), "avg", testMap);

    ApplicationId appId = NamespaceId.DEFAULT.app("newApp2");
    AppRequest<DataQualityApp.DataQualityConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifact.getArtifact(), appArtifact.getVersion()), config);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    ServiceManager serviceManager = applicationManager.getServiceManager
      (DataQualityService.SERVICE_NAME).start();
    serviceManager.waitForStatus(true);

    /* Test for aggregationsGetter handler */

    URL url = new URL(serviceManager.getServiceURL(),
                      "v1/sources/logStream/fields/content_length/aggregations/Mean/timeseries");
    HttpResponse httpResponse = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, httpResponse.getResponseCode());
    String response = httpResponse.getResponseBodyAsString();

    List<TimestampValue> tsValueListActual = GSON.fromJson(response, TOKEN_TYPE_LIST_TIMESTAMP_VALUE);
    TimestampValue firstTimestampValue = tsValueListActual.get(0);
    Assert.assertEquals(256.0, firstTimestampValue.getValue());
    serviceManager.stop();
    serviceManager.waitForFinish(180, TimeUnit.SECONDS);
  }

  @Test
  public void testTotals() throws Exception {
    Map<String, Set<String>> testMap = new HashMap<>();
    Set<String> testSet = new HashSet<>();

    testSet.add("DiscreteValuesHistogram");
    testMap.put("content_length", testSet);
    testMap.put("status", testSet);
    testMap.put("request_time", testSet);

    DataQualityApp.DataQualityConfig config = new DataQualityApp.DataQualityConfig(WORKFLOW_SCHEDULE_MINUTES,
      getStreamSource(), "histogram", testMap);

    ApplicationId appId = NamespaceId.DEFAULT.app("newApp3");
    AppRequest<DataQualityApp.DataQualityConfig> appRequest = new AppRequest<>(
      new ArtifactSummary(appArtifact.getArtifact(), appArtifact.getVersion()), config);
    ApplicationManager applicationManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = applicationManager.getMapReduceManager("FieldAggregator").start();
    mrManager.waitForFinish(180, TimeUnit.SECONDS);

    Map<String, Integer> expectedMap = new HashMap<>();
    expectedMap.put("256", 3);

    /* Test for the aggregationsGetter handler */

    ServiceManager serviceManager = applicationManager.getServiceManager
      (DataQualityService.SERVICE_NAME).start();
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
    expectedSet.add(new FieldDetail("request_time", aggregationTypeValuesList));
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
    serviceManager.stop();
    serviceManager.waitForFinish(180, TimeUnit.SECONDS);
  }

  private DataQualitySource getStreamSource() {
    return getStreamSource("logStream", WORKFLOW_SCHEDULE_MINUTES, "clf");
  }

  private DataQualitySource getStreamSource(String streamName, int workflowMinutes, String format) {
    return getStreamSource(streamName, workflowMinutes, format, null);
  }

  private DataQualitySource getStreamSource(String streamName, int workflowMinutes, @Nullable String format,
                                            @Nullable String schema) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", streamName);
    properties.put("duration", String.valueOf(workflowMinutes) + "m");
    if (!Strings.isNullOrEmpty(format)) {
      properties.put("format", format);
    }
    if (!Strings.isNullOrEmpty(schema)) {
      properties.put("schema", schema);
    }
    return new DataQualitySource("Stream", streamName, properties);
  }
}

