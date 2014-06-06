/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.conf;

import com.continuuity.common.io.Codec;
import com.continuuity.common.zookeeper.store.ZKPropertyStore;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class ZKPropertyStoreTest extends PropertyStoreTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClient;

  @BeforeClass
  public static void init() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
  }

  @AfterClass
  public static void finish() {
    zkClient.stopAndWait();
    zkServer.stopAndWait();
  }

  @Override
  protected <T> PropertyStore<T> createPropertyStore(Codec<T> codec) {
    return ZKPropertyStore.create(zkClient, codec);
  }
}
