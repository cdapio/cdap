/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.app.program.Type;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.test.internal.AppFabricTestHelper;
import com.continuuity.test.internal.DefaultId;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.jar.Manifest;

/**
 * Tests the functionality of Deploy Manager.
 */
public class LocalManagerTest {
  private static LocationFactory lf;

  @BeforeClass
  public static void before() throws Exception {
    lf = new LocalLocationFactory();
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    String jar = JarFinder.getJar(WebCrawlApp.class, new Manifest());
    Location deployedJar = lf.create(jar);
    try {
      AppFabricTestHelper.getLocalManager().deploy(DefaultId.ACCOUNT, null, deployedJar).get();
    } finally {
      deployedJar.delete(true);
    }
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    Location deployedJar = lf.create(
      JarFinder.getJar(ToyApp.class, AppFabricTestHelper.getManifestWithMainClass(ToyApp.class))
    );

    ListenableFuture<?> p = AppFabricTestHelper.getLocalManager().deploy(DefaultId.ACCOUNT, null, deployedJar);
    ApplicationWithPrograms input = (ApplicationWithPrograms) p.get();

    Assert.assertEquals(input.getAppSpecLoc().getArchive(), deployedJar);
    Assert.assertEquals(input.getPrograms().iterator().next().getType(), Type.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getName(), "ToyFlow");
  }

}
