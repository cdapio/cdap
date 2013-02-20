/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.TestHelper;
import com.continuuity.WordCountApp;
import com.continuuity.app.services.AppFabricClient;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.program.StoreModule4Test;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class DeploymentThriftServerTest {
  private static DeploymentThriftServer server;

  @BeforeClass
  public static void beforeClass() {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new StoreModule4Test(),
                                                   new ServicesModule4Test(new CConfiguration()));

    server = injector.getInstance(DeploymentThriftServer.class);
  }

  @Test (timeout = 20000)
  public void basicThriftServerSpinUpTest() throws Exception {
    // ZK is needed to register server
    File tempDir = FileUtils.getTempDirectory();
    new InMemoryZookeeper(tempDir);
    tempDir.deleteOnExit();
    // starting server
    CConfiguration conf = new CConfiguration();
    server.start(new String[]{}, conf);


    File jar = new File(JarFinder.getJar(WordCountApp.class, TestHelper.getManifestWithMainClass(WordCountApp.class)));

    AppFabricClient appFabricClient = new AppFabricClient(new CConfiguration(), false);
    ImmutablePair<Boolean, String> deployResult = appFabricClient.deploy(jar, "account1", "application1");
    // deployed successfully
    Assert.assertTrue(deployResult.getFirst());
  }
}
