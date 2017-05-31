/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.cask360;

import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataTime;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.metrics.RuntimeMetrics;
import co.cask.cdap.cask360.Cask360Service.Cask360ServiceResponse;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Test for {@link Cask360Table}.
 */
public class TestCask360App extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCask360App.class);

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private Gson gson = Cask360Entity.getGson();

  @AfterClass
  public static void stopEverything() throws Exception {
    clear();
    finish();
  }

  @Test
  public void testEverything() throws Exception {

    // Deploy the Cask360Core application
    ApplicationManager appManager = deployApplication(Cask360App.class);

    // Start Service
    LOG.info("Starting service");
    ServiceManager serviceManager = appManager.getServiceManager(Cask360App.SERVICE_NAME).start();

    LOG.info("Waiting for service");

    // Wait service startup
    serviceManager.waitForStatus(true);
    URL serviceURL = serviceManager.getServiceURL();

    LOG.info("\n\nService started at " + serviceURL.toString() + "\n\n");

    DataSetManager<Cask360Table> dataSetManager = getDataset(Cask360App.TABLE_NAME);
    Cask360Table table = dataSetManager.get();

    // Lookup non-existing entity
    String id = "1";
    String entityPath = "entity/";
    Cask360ServiceResponse response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected an error but query was actually successful", response.error == 1);

    // Write an entity directly to the table
    Cask360Entity entity = makeTestEntityOne(id);
    table.write(id, entity);
    int id1count = entity.size();
    dataSetManager.flush();

    // Read entity back directly from table (twice)
    Cask360Entity directEntity = table.read(id);
    directEntity = table.read(id);
    LOG.info("\nWrote : " + entity.toString() + "\nDRead1: " + directEntity.toString());
    Assert.assertTrue("Directly accessed entity different from written entity", entity.equals(directEntity));

    // Read entity back from a new instance of a table (no cache)
    DataSetManager<Cask360Table> dataSetManager2 = getDataset(Cask360App.TABLE_NAME);
    Cask360Table table2 = dataSetManager2.get();
    Cask360Entity directEntity2 = table2.read(id);
    directEntity2 = table.read(id);
    LOG.info("\nWrote : " + entity.toString() + "\nDRead2: " + directEntity2.toString());
    Assert.assertTrue("Directly accessed entity2 different from written entity", entity.equals(directEntity2));

    // Lookup entity again from service
    response = getUrl(serviceURL, entityPath + id);
    LOG.info("\nWrote: " + entity.toString() + "\nSRead: " + response.entity.toString());
    Assert.assertTrue("Expected success but lookup failed", response.error == 0);
    Assert.assertTrue("Returned entity different from written entity", entity.equals(response.entity));

    // Now write twice to one entity and verify the merge

    // Lookup non-existing new entity
    id = "2";
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected an error but query was actually successful", response.error == 1);

    // Perform and verify first write to new entity
    Cask360Entity entityA = makeTestEntityTwo(id);
    table.write(id, entityA);
    dataSetManager.flush();
    Cask360Entity directEntityA = table.read(id);
    Assert.assertTrue("Directly accessed entity different from written entity", entityA.equals(directEntityA));
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected success but lookup failed", response.error == 0);
    Assert.assertTrue("Returned entity different from written entity", entityA.equals(response.entity));

    // Perform and verify second write to new entity is merged result
    Cask360Entity entityB = makeTestEntityTwoOverlap(id);
    Cask360Entity combinedEntity = makeTestEntityTwoCombined(id);
    table.write(id, entityB);
    dataSetManager.flush();
    Cask360Entity directEntityB = table.read(id);
    Assert.assertTrue("Directly accessed entity different from expected merged entity",
        combinedEntity.equals(directEntityB));
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected success but lookup failed", response.error == 0);
    Assert.assertTrue("Returned entity different from written entity", combinedEntity.equals(response.entity));

    // Verify count via SQL

    int id2count = combinedEntity.size();
    int expectedCount = id1count + id2count;

    // 
    Connection conn = getQueryClient();
    Statement st = conn.createStatement();
    ResultSet res = st.executeQuery("SELECT * FROM dataset_" + Cask360App.TABLE_NAME);
    int sqlCount = 0;
    while (res.next()) {
      sqlCount++;
    }
    Assert.assertEquals(expectedCount, sqlCount);

    // Now try writing with the TIME type

    // Attempt to write to existing MAP group and expect failure
    id = "TimeID";
    Cask360Entity entityTime = makeTestEntityTimeOne(id);
    try {
      table.write(id, entityTime);
      Assert.assertTrue("Expected write operation to fail but did not", false);
    } catch (IllegalArgumentException e) {
      LOG.info("Received expected exception", e);
    }

    // Attempt to write TIME data twice to the same entity and verify merge
    Cask360Entity entityTimeA = makeTestEntityTimeTwoA(id);
    Cask360Entity entityTimeB = makeTestEntityTimeTwoB(id);
    Cask360Entity entityTimeAB = makeTestEntityTimeTwoAB(id);

    // Lookup non-existing new entity
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected an error but query was actually successful", response.error == 1);

    // Perform and verify first write to new entity
    table.write(id, entityTimeA);
    dataSetManager.flush();
    Cask360Entity directEntityTimeA = table.read(id);
    LOG.info("\nExpect: " + entityTimeA.toString() + "\nActual: " + directEntityTimeA.toString());
    Assert.assertTrue("Directly accessed entity different from written entity, " +
        "expected=(" + entityTimeA.toString() + ") actual=(" + directEntityTimeA.toString() + ")",
        entityTimeA.equals(directEntityTimeA));
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected success but lookup failed", response.error == 0);
    Assert.assertTrue("Returned entity different from written entity", entityTimeA.equals(response.entity));

    // Perform and verify second write to new entity is merged result
    table.write(id, entityTimeB);
    dataSetManager.flush();
    Cask360Entity directEntityTimeB = table.read(id);
    LOG.info("\nExpect: " + entityTimeAB.toString() + "\nActual: " + directEntityTimeB.toString());
    Assert.assertTrue("Directly accessed entity different from expected merged entity",
        entityTimeAB.equals(directEntityTimeB));
    response = getUrl(serviceURL, entityPath + id);
    Assert.assertTrue("Expected success but lookup failed", response.error == 0);
    Assert.assertTrue("Returned entity different from written entity", entityTimeAB.equals(response.entity));


    // Start the Flow
    FlowManager flowManager = appManager.getFlowManager(Cask360App.FLOW_NAME);
    flowManager.start().waitForStatus(true, 2, 5);

    // Write a couple CSV events for a couple IDs to the stream
    StreamManager streamManager = getStreamManager(Cask360App.STREAM_NAME);
    String csvLine = "3,csv,source,csv";
    streamManager.send(csvLine);
    csvLine = "4,csv,source,csv";
    streamManager.send(csvLine);

    // Write a couple JSON events to the same ID and a new ID to the stream
    String jsonLine = "{'id':'4','data':{'json':{'type':'map','data':" +
        "{'source':'json', 'email':'support@cask.co'}}," +
        "'other':{'type':'map','data':{'okey':'ovalue'}}}}";
    streamManager.send(jsonLine);
    jsonLine = "{'id':'5','data':{'json':{'type':'map','data':{'source':'json'}}}}";
    streamManager.send(jsonLine);

    // Wait for 4 events to be read and written
    RuntimeMetrics readerFlowletMetrics = flowManager.getFlowletMetrics(Cask360Flow.WRITER_FLOWLET_NAME);
    RuntimeMetrics writerFlowletMetrics = flowManager.getFlowletMetrics(Cask360Flow.WRITER_FLOWLET_NAME);
    readerFlowletMetrics.waitForProcessed(4L, 5L, TimeUnit.SECONDS);
    writerFlowletMetrics.waitForProcessed(4L, 5L, TimeUnit.SECONDS);

    // Verify data is as expected
    dataSetManager.flush();
    Cask360Entity expected = new Cask360Entity("3");
    expected.write("csv", "source", "csv");
    Assert.assertTrue("Data written to Stream through Flow to Table not as expected",
        expected.equals(table.read("3")));

    expected = new Cask360Entity("4");
    expected.write("csv", "source", "csv");
    expected.write("json", "source", "json");
    expected.write("json", "email", "support@cask.co");
    expected.write("other", "okey", "ovalue");
    Assert.assertTrue("Data written to Stream through Flow to Table not as expected",
        expected.equals(table.read("4")));

    expected = new Cask360Entity("5");
    expected.write("json", "source", "json");
    Assert.assertTrue("Data written to Stream through Flow to Table not as expected",
        expected.equals(table.read("5")));

    // Now test writing events through the Service

    // Write a couple CSV events for a couple IDs to the Service
    id = "6";
    csvLine = id + ",csv,source,csv";
    response = putUrl(serviceURL, entityPath + id, csvLine);
    Assert.assertTrue("Service write operation failed", response.isSuccess());
    id = "7";
    csvLine = id + ",csv,source,csv";
    response = putUrl(serviceURL, entityPath + id, csvLine);
    Assert.assertTrue("Service write operation failed", response.isSuccess());

    // Write a couple JSON events to the same ID and a new ID to the stream
    id = "7";
    jsonLine = "{'id':'" + id + "','data':{'json':{'type':'map','data':" +
        "{'source':'json', 'email':'support@cask.co'}}," +
        "'other':{'type':'map','data':{'okey':'ovalue'}}}}"; // JSON with ' around keys/vals
    response = putUrl(serviceURL, entityPath + id, jsonLine);
    Assert.assertTrue("Service write operation failed", response.isSuccess());
    id = "8";
    // JSON with " around keys/vals
    jsonLine = "{\"id\":\"" + id + "\",\"data\":{\"json\":{\"type\":\"map\",\"data\":{\"source\":\"json\"}}}}";
    response = putUrl(serviceURL, entityPath + id, jsonLine);
    Assert.assertTrue("Service write operation failed", response.isSuccess());

    // Verify data is as expected
    dataSetManager.flush();
    expected = new Cask360Entity("6");
    expected.write("csv", "source", "csv");
    Assert.assertTrue("Data written to Service to Table not as expected", expected.equals(table.read("6")));

    expected = new Cask360Entity("7");
    expected.write("csv", "source", "csv");
    expected.write("json", "source", "json");
    expected.write("json", "email", "support@cask.co");
    expected.write("other", "okey", "ovalue");
    Assert.assertTrue("Data written to Service to Table not as expected", expected.equals(table.read("7")));

    expected = new Cask360Entity("8");
    expected.write("json", "source", "json");
    Assert.assertTrue("Data written to Service to Table not as expected", expected.equals(table.read("8")));

    // Shut everything down
    flowManager.stop();
    serviceManager.stop();
    appManager.stopAll();
  }

  private Cask360ServiceResponse getUrl(URL baseURL, String path) throws MalformedURLException, IOException {
    LOG.info("GET to " + baseURL + path);
    HttpURLConnection connection = (HttpURLConnection) new URL(baseURL, path).openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    LOG.info("Response: " + response);
    return gson.fromJson(response, Cask360ServiceResponse.class);
  }

  private Cask360ServiceResponse putUrl(URL baseURL, String path, String data)
      throws MalformedURLException, IOException {
    LOG.info("PUT to " + baseURL + path + " with data " + data);
    HttpURLConnection connection = (HttpURLConnection) new URL(baseURL, path).openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    OutputStreamWriter output =
        new OutputStreamWriter(connection.getOutputStream());
    output.write(data);
    output.close();
    String response;
    try {
      response = new String(ByteStreams.toByteArray(connection.getInputStream()), Charsets.UTF_8);
    } finally {
      connection.disconnect();
    }
    LOG.info("Response: " + response);
    Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    return gson.fromJson(response, Cask360ServiceResponse.class);
  }

  private static Cask360Entity makeTestEntityOne(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapOne = new TreeMap<String, String>();
    mapOne.put("a", "b");
    mapOne.put("c", "d");
    mapOne.put("e", "f");
    testData.put("one", new Cask360Group("one", new Cask360GroupDataMap(mapOne)));
    Map<String, String> mapTwo = new TreeMap<String, String>();
    mapTwo.put("g", "h");
    testData.put("two", new Cask360Group("two", new Cask360GroupDataMap(mapTwo)));
    Map<String, String> mapThree = new TreeMap<String, String>();
    mapThree.put("i", "j");
    mapThree.put("k", "l");
    testData.put("three", new Cask360Group("three", new Cask360GroupDataMap(mapThree)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTwo(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "f");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "l");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTwoOverlap(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTwoCombined(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTimeOne(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "aV");
    mapOne1.put("bK", "bV");
    mapOne1.put("cK", "cV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "aV");
    mapOne2.put("cK", "cV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "zV");
    mapOne3.put("cK", "cV");
    mapOne.put(789L, mapOne3);
    testData.put("one", new Cask360Group("one", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(2L, mapFour1);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTimeTwoA(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "aV");
    mapOne1.put("bK", "bV");
    mapOne1.put("cK", "cV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "aV");
    mapOne2.put("cK", "cV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "zV");
    mapOne3.put("cK", "cV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(2L, mapFour1);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    return new Cask360Entity(id, testData);
  }

  private static final long now_ms = System.currentTimeMillis();
  private static final long now_ns = System.nanoTime();

  private static Cask360Entity makeTestEntityTimeTwoB(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("cK", "CV");
    mapOne1.put("aK", "AV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "AV");
    mapOne2.put("dK", "DV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "ZV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(1L, mapFour1);
    Map<String, String> mapFour2 = new TreeMap<String, String>();
    mapFour2.put("xK", "xV");
    mapFour2.put("yK", "yV");
    mapFour.put(3L, mapFour2);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    Map<Long, Map<String, String>> mapFive = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFive1 = new TreeMap<String, String>();
    mapFive1.put("xK", "xV");
    mapFive1.put("yK", "yV");
    mapFive.put(now_ms, mapFive1);
    Map<String, String> mapFive2 = new TreeMap<String, String>();
    mapFive2.put("xK", "xV");
    mapFive2.put("yK", "yV");
    mapFive.put(now_ns, mapFive2);
    testData.put("Tfive", new Cask360Group("Tfive", new Cask360GroupDataTime(mapFive)));
    return new Cask360Entity(id, testData);
  }

  private static Cask360Entity makeTestEntityTimeTwoAB(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();

    Map<Long, Map<String, String>> mapOne = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapOne1 = new TreeMap<String, String>();
    mapOne1.put("aK", "AV");
    mapOne1.put("cK", "CV");
    mapOne.put(123L, mapOne1);
    Map<String, String> mapOne2 = new TreeMap<String, String>();
    mapOne2.put("aK", "AV");
    mapOne2.put("dK", "DV");
    mapOne.put(456L, mapOne2);
    Map<String, String> mapOne3 = new TreeMap<String, String>();
    mapOne3.put("zK", "ZV");
    mapOne.put(789L, mapOne3);
    testData.put("Tone", new Cask360Group("Tone", new Cask360GroupDataTime(mapOne)));
    Map<Long, Map<String, String>> mapFour = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFour1 = new TreeMap<String, String>();
    mapFour1.put("xK", "xV");
    mapFour1.put("yK", "yV");
    mapFour.put(1L, mapFour1);
    Map<String, String> mapFour2 = new TreeMap<String, String>();
    mapFour2.put("xK", "xV");
    mapFour2.put("yK", "yV");
    mapFour.put(3L, mapFour2);
    Map<String, String> mapFour3 = new TreeMap<String, String>();
    mapFour3.put("xK", "xV");
    mapFour3.put("yK", "yV");
    mapFour.put(2L, mapFour3);
    testData.put("Tfour", new Cask360Group("Tfour", new Cask360GroupDataTime(mapFour)));
    Map<Long, Map<String, String>> mapFive = new TreeMap<Long, Map<String, String>>().descendingMap();
    Map<String, String> mapFive1 = new TreeMap<String, String>();
    mapFive1.put("xK", "xV");
    mapFive1.put("yK", "yV");
    mapFive.put(now_ms, mapFive1);
    Map<String, String> mapFive2 = new TreeMap<String, String>();
    mapFive2.put("xK", "xV");
    mapFive2.put("yK", "yV");
    mapFive.put(now_ns, mapFive2);
    testData.put("Tfive", new Cask360Group("Tfive", new Cask360GroupDataTime(mapFive)));
    return new Cask360Entity(id, testData);
  }
}
