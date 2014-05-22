/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.common.conf.Constants;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class AppFabricServerTest {
  private static AppFabricServer server;
  private static DiscoveryServiceClient discoveryServiceClient;

  @BeforeClass
  public static void before() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    server = injector.getInstance(AppFabricServer.class);
    discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
  }

  @Test
  public void startStopServer() throws Exception {
    Service.State state = server.startAndWait();
    Assert.assertTrue(state == Service.State.RUNNING);
    // Sleep shortly for the discovery
    TimeUnit.SECONDS.sleep(1);
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.APP_FABRIC_HTTP);
    Assert.assertFalse(Iterables.isEmpty(discovered));

    state = server.stopAndWait();
    Assert.assertTrue(state == Service.State.TERMINATED);

    TimeUnit.SECONDS.sleep(1);
    Assert.assertTrue(Iterables.isEmpty(discovered));
  }
}
