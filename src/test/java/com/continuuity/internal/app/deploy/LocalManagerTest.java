/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.TestHelper;
import com.continuuity.ToyApp;
import com.continuuity.WebCrawlApp;
import com.continuuity.app.deploy.Manager;
import com.continuuity.app.program.Id;
import com.continuuity.app.program.Store;
import com.continuuity.app.program.Type;
import com.continuuity.archive.JarFinder;
import com.continuuity.common.conf.Configuration;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.filesystem.Location;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.app.deploy.pipeline.ApplicationSpecLocation;
import com.continuuity.internal.app.deploy.pipeline.ApplicationWithPrograms;
import com.continuuity.internal.app.deploy.pipeline.VerificationStage;
import com.continuuity.internal.app.program.MDSBasedStore;
import com.continuuity.internal.app.program.MDSBasedStoreTest;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.continuuity.internal.pipeline.SynchronousPipelineFactory;
import com.continuuity.metadata.thrift.MetadataService;
import com.continuuity.pipeline.PipelineFactory;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configured;
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
    TestHelper.getLocalManager().deploy(Id.Account.DEFAULT(), deployedJar);
  }

  /**
   * Good pipeline with good tests.
   */
  @Test
  public void testGoodPipeline() throws Exception {
    Location deployedJar = lf.create(
      JarFinder.getJar(ToyApp.class, TestHelper.getManifestWithMainClass(ToyApp.class))
    );

    ListenableFuture<?> p = TestHelper.getLocalManager().deploy(Id.Account.DEFAULT(), deployedJar);
    ApplicationWithPrograms input = (ApplicationWithPrograms)p.get();

    Assert.assertEquals(input.getAppSpecLoc().getArchive(), deployedJar);
    Assert.assertEquals(input.getPrograms().iterator().next().getProcessorType(), Type.FLOW);
    Assert.assertEquals(input.getPrograms().iterator().next().getProcessorName(), "ToyFlow");
  }

}
