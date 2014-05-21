/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.handlers.metrics;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.Id;
import com.continuuity.app.store.Store;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.DiscoveryRuntimeModule;
import com.continuuity.common.guice.LocationRuntimeModule;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.MetricsServiceTestsSuite;
import com.continuuity.gateway.apps.wordcount.WCount;
import com.continuuity.gateway.apps.wordcount.WordCount;
import com.continuuity.gateway.handlers.log.MockLogReader;
import com.continuuity.internal.app.Specifications;
import com.continuuity.logging.read.LogReader;
import com.continuuity.metrics.guice.MetricsClientRuntimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 *
 */
public class BaseMetricsQueryTest {

  private static File dataDir;
  private static Id.Application wordCountAppId;
  private static Id.Application wCountAppId;

  protected static MetricsCollectionService collectionService;
  protected static Store store;
  protected static LocationFactory locationFactory;
  protected static List<String> validResources;
  protected static List<String> invalidResources;

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws IOException, OperationException {
    dataDir = tempFolder.newFolder();

    CConfiguration cConf = CConfiguration.create();

    // use this injector instead of the one in MetricsServiceTestsSuite because that one uses a
    // mock metrics collection service while we need a real one.
    Injector injector = Guice.createInjector(Modules.override(
      new ConfigModule(cConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new DiscoveryRuntimeModule().getInMemoryModules(),
      new MetricsClientRuntimeModule().getInMemoryModules(),
      new DataFabricModules().getInMemoryModules()
    ).with(new AbstractModule() {
      @Override
      protected void configure() {
        bind(LogReader.class).to(MockLogReader.class).in(Scopes.SINGLETON);
      }
    }));

    collectionService = injector.getInstance(MetricsCollectionService.class);
    collectionService.startAndWait();

    // MetricsTestsSuite starts app-fabric and has all its dependencies set up.
    StoreFactory storeFactory = MetricsServiceTestsSuite.getInjector().getInstance(StoreFactory.class);
    store = storeFactory.create();
    locationFactory = MetricsServiceTestsSuite.getInjector().getInstance(LocationFactory.class);

    setupMeta();
  }

  @AfterClass
  public static void finish() throws OperationException {
    collectionService.stopAndWait();
    store.removeApplication(wordCountAppId);
    store.removeApplication(wCountAppId);

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

  // write WordCount app to metadata store
  public static void setupMeta() throws OperationException {

    String account = Constants.DEVELOPER_ACCOUNT_ID;
    Location appArchiveLocation = locationFactory.getHomeLocation();

    // write WordCount application to meta store
    Application app = new WordCount();
    ApplicationSpecification appSpec = Specifications.from(app.configure());
    wordCountAppId = new Id.Application(new Id.Account(account), appSpec.getName());
    store.addApplication(wordCountAppId, appSpec, appArchiveLocation);

    // write WCount application to meta store
    app = new WCount();
    appSpec = Specifications.from(app.configure());
    wCountAppId = new Id.Application(new Id.Account(account), appSpec.getName());
    store.addApplication(wCountAppId, appSpec, appArchiveLocation);

    validResources = ImmutableList.of(
      "/reactor/reads?aggregate=true",
      "/reactor/apps/WordCount/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/datasets/wordStats/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/streams/wordStream/collect.events?aggregate=true",
      "/reactor/cluster/resources.total.storage?aggregate=true"
    );

    invalidResources = ImmutableList.of(
      // malformed context format.  for example, bad scope or bad spelling of 'flow' instead of 'flows'
      "/reacto/reads?aggregate=true",
      "/reactor/app/WordCount/reads?aggregate=true",
      "/reactor/apps/WordCount/flow/WordCounter/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlet/counter/reads?aggregate=true",
      "/reactor/dataset/wordStats/reads?aggregate=true",
      "/reactor/datasets/wordStats/app/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flow/counter/reads?aggregate=true",
      "/reactor/datasets/wordStats/apps/WordCount/flows/WordCounter/flowlet/counter/reads?aggregate=true",
      // context format is fine but path elements (datsets, streams, programs) are non-existant
      "/reactor/apps/WordCont/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCouner/reads?aggregate=true",
      "/reactor/apps/WordCount/flows/WordCounter/flowlets/couter/reads?aggregate=true",
      "/reactor/datasets/wordStat/reads?aggregate=true",
      "/reactor/datasets/wordStat/apps/WordCount/reads?aggregate=true",
      "/reactor/datasets/wordStas/apps/WordCount/flows/WordCounter/reads?aggregate=true",
      "/reactor/datasets/wordStts/apps/WordCount/flows/WordCounter/flowlets/counter/reads?aggregate=true",
      "/reactor/streams/wordStrea/collect.events?aggregate=true"
    );
  }
}
