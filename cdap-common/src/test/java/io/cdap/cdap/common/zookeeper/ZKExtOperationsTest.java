/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.common.zookeeper;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Codec;
import com.google.common.base.Function;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tests for testing helper methods in {@link ZKExtOperations}
 */
public class ZKExtOperationsTest {

  private static final Codec<Integer> INT_CODEC = new Codec<Integer>() {
    @Override
    public byte[] encode(Integer object) throws IOException {
      return Bytes.toBytes(object);
    }

    @Override
    public Integer decode(byte[] data) throws IOException {
      return Bytes.toInt(data);
    }
  };

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static InMemoryZKServer zkServer;


  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();
  }

  @Test
  public void testGetAndSet() throws Exception {
    String path = "/testGetAndSet";
    ZKClientService zkClient1 = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    ZKClientService zkClient2 = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();

    zkClient1.startAndWait();
    zkClient2.startAndWait();

    // First a node would get created since no node there.
    ZKExtOperations.updateOrCreate(zkClient1, path, new Function<Integer, Integer>() {

      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        Assert.assertNull(input);
        return 0;
      }
    }, INT_CODEC).get(10, TimeUnit.SECONDS);

    // Use a 2nd client to do modification
    ZKExtOperations.updateOrCreate(zkClient2, path, new Function<Integer, Integer>() {

      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        Assert.assertEquals(0, input.intValue());
        return 1;
      }
    }, INT_CODEC).get(10, TimeUnit.SECONDS);

    // Use both client to do concurrent modification. Make sure they fetched the same value before performing set
    final CyclicBarrier barrier = new CyclicBarrier(2);
    Function<Integer, Integer> modifier = new Function<Integer, Integer>() {

      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        try {
          if (input == 1) {
            barrier.await();
            return 2;
          } else if (input == 2) {
            return 3;
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
        throw new IllegalStateException("Illegal input " + input);
      }
    };
    Future<Integer> future1 = ZKExtOperations.updateOrCreate(zkClient1, path, modifier, INT_CODEC);
    Future<Integer> future2 = ZKExtOperations.updateOrCreate(zkClient2, path, modifier, INT_CODEC);

    int r1 = future1.get(10, TimeUnit.SECONDS);
    int r2 = future2.get(10, TimeUnit.SECONDS);

    // One of the result should be 2, the other should be 3. The order may vary
    Assert.assertTrue((r1 == 2 && r2 == 3) || (r1 == 3 && r2 == 2));

    // Not doing update by returning null in modifier
    Integer result = ZKExtOperations.updateOrCreate(zkClient1, path, new Function<Integer, Integer>() {

      @Nullable
      @Override
      public Integer apply(@Nullable Integer input) {
        return (input == 3) ? null : 4;
      }
    }, INT_CODEC).get();

    Assert.assertNull(result);

    zkClient1.stopAndWait();
    zkClient2.stopAndWait();
  }

  @Test
  public void testCreateOrSet() throws Exception {
    String path = "/parent/testCreateOrSet";
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    // Create with "1"
    Assert.assertEquals(1, ZKExtOperations.createOrSet(zkClient, path,
                                                       Suppliers.ofInstance(1), INT_CODEC, 0).get().intValue());
    // Should get "1" back
    Assert.assertEquals(1, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    // Set with "2"
    Assert.assertEquals(2, ZKExtOperations.createOrSet(zkClient, path,
                                                       Suppliers.ofInstance(2), INT_CODEC, 0).get().intValue());
    // Should get "2" back
    Assert.assertEquals(2, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    zkClient.stopAndWait();
  }

  @Test
  public void testSetOrCreate() throws Exception {
    String path = "/parent/testSetOrCreate";
    ZKClientService zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    // Create with "1"
    Assert.assertEquals(1, ZKExtOperations.setOrCreate(zkClient, path,
                                                       Suppliers.ofInstance(1), INT_CODEC, 0).get().intValue());
    // Should get "1" back
    Assert.assertEquals(1, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    // Set with "2"
    Assert.assertEquals(2, ZKExtOperations.setOrCreate(zkClient, path,
                                                       Suppliers.ofInstance(2), INT_CODEC, 0).get().intValue());
    // Should get "2" back
    Assert.assertEquals(2, INT_CODEC.decode(zkClient.getData(path).get().getData()).intValue());

    zkClient.stopAndWait();
  }

  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }
}
