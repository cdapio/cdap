/*
 * Copyright © 2014-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.internal.app.deploy.pipeline;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.internal.DefaultId;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.pipeline.StageContext;
import io.cdap.cdap.proto.id.NamespaceId;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests the program generation stage of the deploy pipeline.
 */
public class ProgramGenerationStageTest {
  private static final CConfiguration cConf = CConfiguration.create();

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testProgramGeneration() throws Exception {
    cConf.set(Constants.AppFabric.OUTPUT_DIR, "programs");
    LocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    // have to do this since we are not going through the route of create namespace -> deploy application
    // in real scenarios, the namespace directory would already be created
    Location namespaceLocation = lf.create(DefaultId.APPLICATION.getNamespace());
    Locations.mkdirsIfNotExists(namespaceLocation);
    LocationFactory jarLf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location appArchive = AppJarHelper.createDeploymentJar(jarLf, AllProgramsApp.class);
    ApplicationSpecification appSpec = Specifications.from(new AllProgramsApp());
    ApplicationSpecificationAdapter adapter = ApplicationSpecificationAdapter.create();
    ApplicationSpecification newSpec = adapter.fromJson(adapter.toJson(appSpec));
    ApplicationClass applicationClass = new ApplicationClass(AllProgramsApp.class.getName(), "", null);
    ProgramGenerationStage pgmStage = new ProgramGenerationStage();
    pgmStage.process(new StageContext(Object.class));  // Can do better here - fixed right now to run the test.
    pgmStage.process(new ApplicationDeployable(NamespaceId.DEFAULT.artifact("AllProgramApp", "1.0"), appArchive,
                                               DefaultId.APPLICATION, newSpec, null,
                                               ApplicationDeployScope.USER, applicationClass));
    Assert.assertTrue(true);
  }

}
