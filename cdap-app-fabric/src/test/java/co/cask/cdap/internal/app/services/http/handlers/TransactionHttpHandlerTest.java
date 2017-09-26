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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.hbase.txprune.InvalidListPruningDebugTool;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.RegionsAtTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Transaction handlers.
 */
public class TransactionHttpHandlerTest extends AppFabricTestBase {

  private static final Type STRING_INT_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final Type SET_STRING_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type SET_PRUNE_INFO_TYPE = new TypeToken<Set<? extends RegionPruneInfo>>() { }.getType();
  private static final Comparator<RegionPruneInfo> PRUNE_INFO_COMPARATOR =
    new Comparator<RegionPruneInfo>() {
      @Override
      public int compare(RegionPruneInfo o1, RegionPruneInfo o2) {
        return Long.compare(o1.getPruneUpperBound(), o2.getPruneUpperBound());
      }
    };

  @Before
  public void clear() {
    InvalidListPruningDebugTool.reset();
  }

  /**
   * Tests invalidating a transaction.
   */
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient txClient = getTxClient();

    Transaction tx1 = txClient.startShort();
    HttpResponse response = doPost("/v3/transactions/" + tx1.getWritePointer() + "/invalidate");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Transaction tx2 = txClient.startShort();
    txClient.commitOrThrow(tx2);
    response = doPost("/v3/transactions/" + tx2.getWritePointer() + "/invalidate");
    Assert.assertEquals(409, response.getStatusLine().getStatusCode());

    Assert.assertEquals(400,
                        doPost("/v3/transactions/foobar/invalidate").getStatusLine().getStatusCode());
  }

  @Test
  public void testResetTxManagerState() throws Exception {
    HttpResponse response = doPost("/v3/transactions/state");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testPruneNow() throws Exception {
    HttpResponse response = doPost("/v3/transactions/prune/now");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testTruncateInvalidTx() throws Exception {
    TransactionSystemClient txClient = getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());
    
    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    Transaction tx3 = txClient.startLong();
    
    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));
    
    Assert.assertEquals(3, txClient.getInvalidSize());
    
    // Remove tx1 and tx3 from invalid list
    HttpResponse response = 
      doPost("/v3/transactions/invalid/remove/ids",
             GSON.toJson(ImmutableMap.of("ids", ImmutableSet.of(tx1.getWritePointer(), tx3.getWritePointer()))));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    
    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testTruncateInvalidTxBefore() throws Exception {
    TransactionSystemClient txClient = getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());

    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    // Sleep so that transaction ids get generated a millisecond apart for assertion
    // TEPHRA-63 should eliminate the need to sleep
    TimeUnit.MILLISECONDS.sleep(1);
    long beforeTx3 = System.currentTimeMillis();
    Transaction tx3 = txClient.startLong();

    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));

    Assert.assertEquals(3, txClient.getInvalidSize());

    // Remove all transactions in invalid list beforeTx3
    HttpResponse response =
      doPost("/v3/transactions/invalid/remove/until",
             GSON.toJson(ImmutableMap.of("time", beforeTx3)));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    Assert.assertEquals(1, txClient.getInvalidSize());
  }

  @Test
  public void testGetInvalidSize() throws Exception {
    TransactionSystemClient txClient = getTxClient();
    // Reset state, and assert no invalid transactions are present
    txClient.resetState();
    Assert.assertEquals(0, txClient.getInvalidSize());

    // Start few transactions and invalidate them
    Transaction tx1 = txClient.startShort();
    Transaction tx2 = txClient.startLong();
    Transaction tx3 = txClient.startLong();

    Assert.assertTrue(txClient.invalidate(tx1.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx2.getWritePointer()));
    Assert.assertTrue(txClient.invalidate(tx3.getWritePointer()));

    Assert.assertEquals(3, txClient.getInvalidSize());

    // Assert through REST API
    HttpResponse response = doGet("/v3/transactions/invalid/size");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Map<String, Integer> resultMap = GSON.fromJson(EntityUtils.toString(response.getEntity()), STRING_INT_TYPE);
    Assert.assertNotNull(resultMap);
    Assert.assertEquals(3, (int) resultMap.get("size"));
  }

  @Test
  public void testGetRegionsToBeCompacted() throws Exception {
    Map<String, SortedSet<String>> testData = new HashMap<>();
    testData.put("now", ImmutableSortedSet.of("r1", "r2", "r3", "r4"));
    testData.put("now-20s", ImmutableSortedSet.of("r11", "r12", "r13", "r14"));
    InvalidListPruningDebugTool.setRegionsToBeCompacted(testData);

    HttpResponse response = doGet("/v3/transactions/prune/regions/block");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Set<String> actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), SET_STRING_TYPE);
    Assert.assertEquals(testData.get("now"), actual);

    response = doGet("/v3/transactions/prune/regions/block?time=now-20s&limit=2");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), SET_STRING_TYPE);
    Assert.assertEquals(Sets.newTreeSet(Iterables.limit(testData.get("now-20s"), 2)), actual);
  }

  @Test
  public void testGetIdleRegions() throws Exception {
    Map<String, SortedSet<? extends RegionPruneInfo>> testData = new HashMap<>();
    testData.put("now",
                 pruneInfoSet(new RegionPruneInfo(new byte[] {'a'}, "a", 100, 1000),
                              new RegionPruneInfo(new byte[] {'b'}, "b", 200, 2000))
                 );
    testData.put("now+1h",
                 pruneInfoSet(new RegionPruneInfo(new byte[] {'x'}, "x", 700, 7000),
                              new RegionPruneInfo(new byte[] {'y'}, "y", 800, 8000),
                              new RegionPruneInfo(new byte[] {'z'}, "z", 900, 9000))
    );
    InvalidListPruningDebugTool.setIdleRegions(testData);

    HttpResponse response = doGet("/v3/transactions/prune/regions/idle");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Set<? extends RegionPruneInfo> actual =
      GSON.fromJson(EntityUtils.toString(response.getEntity()), SET_PRUNE_INFO_TYPE);
    Assert.assertEquals(testData.get("now"), ImmutableSortedSet.copyOf(PRUNE_INFO_COMPARATOR, actual));

    response = doGet("/v3/transactions/prune/regions/idle?time=now%2B1h&limit=2");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), SET_PRUNE_INFO_TYPE);
    Iterable<? extends RegionPruneInfo> expectedHour =
      ImmutableSortedSet.copyOf(PRUNE_INFO_COMPARATOR, Iterables.limit(testData.get("now+1h"), 2));
    Assert.assertEquals(expectedHour, ImmutableSortedSet.copyOf(PRUNE_INFO_COMPARATOR, actual));
  }

  @Test
  public void testRegionPruneInfo() throws Exception {
    Map<String, RegionPruneInfo> testData = new HashMap<>();
    testData.put("r", new RegionPruneInfo(new byte[] {'r'}, "r", 100, 1000));
    testData.put("s", new RegionPruneInfo(new byte[] {'s'}, "s", 100, 1000));
    InvalidListPruningDebugTool.setRegionPruneInfos(testData);

    HttpResponse response = doGet("/v3/transactions/prune/regions/r");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    RegionPruneInfo actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), RegionPruneInfo.class);
    Assert.assertEquals(testData.get("r").getRegionNameAsString(), actual.getRegionNameAsString());

    response = doGet("/v3/transactions/prune/regions/non-existent");
    Assert.assertEquals(404, response.getStatusLine().getStatusCode());
  }

  @Test
  public void testGetRegionsOnOrBeforeTime() throws Exception {
    Map<String, RegionsAtTime> testData = new HashMap<>();
    SimpleDateFormat dateFormat = new SimpleDateFormat();
    testData.put("now", new RegionsAtTime(1, ImmutableSortedSet.of("d", "e"), dateFormat));
    testData.put("1234567", new RegionsAtTime(1, ImmutableSortedSet.of("2", "3", "4"), dateFormat));
    InvalidListPruningDebugTool.setRegionsAtTime(testData);

    HttpResponse response = doGet("/v3/transactions/prune/regions");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    RegionsAtTime actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), RegionsAtTime.class);
    Assert.assertEquals(testData.get("now"), actual);

    response = doGet("/v3/transactions/prune/regions?time=1234567");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    actual = GSON.fromJson(EntityUtils.toString(response.getEntity()), RegionsAtTime.class);
    Assert.assertEquals(testData.get("1234567"), actual);
  }

  private SortedSet<? extends RegionPruneInfo> pruneInfoSet(RegionPruneInfo... regionPruneInfos) {
    SortedSet<RegionPruneInfo> pruneInfos = new TreeSet<>(PRUNE_INFO_COMPARATOR);
    Collections.addAll(pruneInfos, regionPruneInfos);
    return pruneInfos;
  }
}
