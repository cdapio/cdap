/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.TestHelper;
import com.continuuity.WebCrawlApp;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.program.Id;
import com.continuuity.archive.JarFinder;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationSpecLocation;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
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
  private static Manager mgr;

  @BeforeClass
  public static void before() throws Exception {
    lf = new LocalLocationFactory();
    mgr = new LocalManager(new SynchronousPipelineFactory());
  }

  /**
   * Improper Manifest file should throw an exception.
   */
  @Test(expected = ExecutionException.class)
  public void testImproperOrNoManifestFile() throws Exception {
    String jar = JarFinder.getJar(WebCrawlApp.class, new Manifest());
    Location deployedJar = lf.create(jar);
    mgr.deploy(Id.Account.DEFAULT(), deployedJar);
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    String jar = JarFinder.getJar(WebCrawlApp.class,
                                  TestHelper.getManifestWithMainClass(WebCrawlApp.class));
    Location deployedJar = lf.create(jar);
    ListenableFuture<?> p = mgr.deploy(Id.Account.DEFAULT(), deployedJar);
    ApplicationSpecLocation input = (ApplicationSpecLocation)p.get();
    Assert.assertEquals(input.getArchive(), deployedJar);
  }

}
