/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.ToyApp;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.lang.jar.JarFinder;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.pipeline.StageContext;
import com.continuuity.test.internal.DefaultId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the program generation stage of the deploy pipeline.
 */
public class ProgramGenerationStageTest {
  private static CConfiguration configuration = CConfiguration.create();

  @Test
  public void testProgramGenerationForToyApp() throws Exception {
    configuration.set(Constants.AppFabric.OUTPUT_DIR, System.getProperty("java.io.tmpdir"));
    LocationFactory lf = new LocalLocationFactory();
    Location appArchive = lf.create(JarFinder.getJar(ToyApp.class));
    ApplicationSpecification appSpec = Specifications.from(new ToyApp().configure());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator());
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ProgramGenerationStage pgmStage = new ProgramGenerationStage(configuration, lf);
    pgmStage.process(new StageContext(Object.class));  // Can do better here - fixed right now to run the test.
    pgmStage.process(new ApplicationSpecLocation(DefaultId.APPLICATION, newSpec, appArchive));
    Assert.assertTrue(true);
  }

}
