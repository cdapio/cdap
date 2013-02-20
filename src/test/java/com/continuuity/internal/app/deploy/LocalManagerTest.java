/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.TestHelper;
import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.app.Id;
import com.continuuity.app.program.Type;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.jar.Manifest;

/**
 * Tests the functionality of Deploy Manager.
 */
public class LocalManagerTest {
  private static LocationFactory lf;
  private static CConfiguration configuration;

  @BeforeClass
  public static void before() throws Exception {
    lf = new LocalLocationFactory();
    configuration = CConfiguration.create();
    configuration.set("app.temp.dir", "/tmp");
    configuration.set("app.output.dir", "/tmp/" + UUID.randomUUID());
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    String jar = JarFinder.getJar(WebCrawlApp.class, new Manifest());
    Location deployedJar = lf.create(jar);
    deployedJar.deleteOnExit();
    TestHelper.getLocalManager(configuration).deploy(Id.Account.DEFAULT(), deployedJar);
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    Location deployedJar = lf.create(
      JarFinder.getJar(ToyApp.class, TestHelper.getManifestWithMainClass(ToyApp.class))
    );

    ListenableFuture<?> p = TestHelper.getLocalManager(configuration).deploy(Id.Account.DEFAULT(), deployedJar);
    ApplicationWithPrograms input = (ApplicationWithPrograms)p.get();

    Assert.assertEquals(input.getAppSpecLoc().getArchive(), deployedJar);
    Assert.assertEquals(input.getPrograms().iterator().next().getProcessorType(), Type.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getProgramName(), "ToyFlow");
  }

}
