/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.test.internal.TempFolder;
import com.continuuity.test.internal.guice.AppFabricTestModule;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.internal.utils.Networks;
import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppFabricServerTest {
  private static AppFabricServer server;
  private static CConfiguration configuration;

  @BeforeClass
  public static void before() throws Exception {
    TempFolder tempFolder = new TempFolder();
    configuration = CConfiguration.create();
    configuration.setInt(Constants.CFG_APP_FABRIC_SERVER_PORT, Networks.getRandomPort());
    configuration.set("app.output.dir", tempFolder.newFolder("app").getAbsolutePath());
    configuration.set("app.tmp.dir", tempFolder.newFolder("temp").getAbsolutePath());

    Injector injector = Guice.createInjector(new AppFabricTestModule(configuration));

    server = injector.getInstance(AppFabricServer.class);
  }

  @Test
  public void startStopServer() throws Exception {
    Service.State state = server.startAndWait();
    Assert.assertTrue(state == Service.State.RUNNING);
    TimeUnit.SECONDS.sleep(5);
    state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);
  }
}
