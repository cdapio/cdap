/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.app.program.Type;
import com.continuuity.archive.JarFinder;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.test.internal.DefaultId;
import com.continuuity.test.internal.TestHelper;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.common.util.concurrent.ListenableFuture;
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
      TestHelper.getLocalManager().deploy(DefaultId.ACCOUNT, deployedJar).get();
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
      JarFinder.getJar(ToyApp.class, TestHelper.getManifestWithMainClass(ToyApp.class))
    );

    ListenableFuture<?> p = TestHelper.getLocalManager().deploy(DefaultId.ACCOUNT, deployedJar);
    ApplicationWithPrograms input = (ApplicationWithPrograms) p.get();

    Assert.assertEquals(input.getAppSpecLoc().getArchive(), deployedJar);
    Assert.assertEquals(input.getPrograms().iterator().next().getProcessorType(), Type.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getProgramName(), "ToyFlow");
  }

}
