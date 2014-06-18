/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.io.Codec;
import com.google.common.base.Function;
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


  @AfterClass
  public static void finish() {
    zkServer.stopAndWait();
  }
}
