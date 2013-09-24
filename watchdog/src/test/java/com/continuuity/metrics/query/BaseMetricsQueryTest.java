/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricLevelDBModule;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.continuuity.metrics.guice.MetricsQueryModule;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class BaseMetricsQueryTest {

  private static File dataDir;
  private static Injector injector;
  protected static MetricsCollectionService collectionService;
  protected static MetricsQueryService queryService;

  protected InetSocketAddress getMetricsQueryEndpoint() throws InterruptedException {
    Iterable<Discoverable> endpoints = injector.getInstance(DiscoveryServiceClient.class)
                                               .discover(Constants.Service.METRICS);
    Iterator<Discoverable> itor = endpoints.iterator();
    while (!itor.hasNext()) {
      TimeUnit.SECONDS.sleep(1);
      itor = endpoints.iterator();
    }
    return itor.next().getSocketAddress();
  }

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws IOException {
    dataDir = tempFolder.newFolder();

    CConfiguration cConf = CConfiguration.create();
    cConf.set(MetricsConstants.ConfigKeys.SERVER_PORT, "0");
    cConf.set(Constants.CFG_DATA_LEVELDB_DIR, dataDir.getAbsolutePath());

    injector = Guice.createInjector(
      new ConfigModule(cConf),
      new DataFabricLevelDBModule(cConf),
      new LocationRuntimeModule().getSingleNodeModules(),
      new DiscoveryRuntimeModule().getSingleNodeModules(),
      new MetricsClientRuntimeModule().getSingleNodeModules(),
      new MetricsQueryModule()
    );

    collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();

    queryService = injector.getInstance(MetricsQueryService.class);
    queryService.startAndWait();
  }

  @AfterClass
  public static void finish() {
    queryService.stopAndWait();
    collectionService.stopAndWait();

    Deque<File> files = Lists.newLinkedList();
    files.add(dataDir);

    File file = files.peekLast();
    while (file != null) {
      File[] children = file.listFiles();
      if (children == null || children.length == 0) {
        files.pollLast().delete();
      } else {
        Collections.addAll(files, children);
      }
      file = files.peekLast();
    }
  }
}
