/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.MissingMapReduceWorkflowApp;
import co.cask.cdap.WordCountApp;
import co.cask.cdap.common.ArtifactNotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.io.Files;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

/**
 */
public class ApplicationLifecycleServiceTest extends AppFabricTestBase {

  private static final String WORDCOUNT_APP_NAME = "WordCountApp";
  private static final String WORDCOUNT_FLOW_NAME = "WordCountFlow";

  private static ApplicationLifecycleService applicationLifecycleService;
  private static LocationFactory locationFactory;
  private static ArtifactRepository artifactRepository;

  @BeforeClass
  public static void setup() throws Exception {
    applicationLifecycleService = getInjector().getInstance(ApplicationLifecycleService.class);
    locationFactory = getInjector().getInstance(LocationFactory.class);
    artifactRepository = getInjector().getInstance(ArtifactRepository.class);
  }

  // test that the call to deploy an artifact and application in a single step will delete the artifact
  // if the application could not be created
  @Test(expected = ArtifactNotFoundException.class)
  public void testDeployArtifactAndApplicationCleansUpArtifactOnFailure() throws Exception {
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "missing-mr", "1.0.0-SNAPSHOT");
    Location appJar = AppJarHelper.createDeploymentJar(locationFactory, MissingMapReduceWorkflowApp.class);
    File appJarFile = new File(tmpFolder.newFolder(),
                               String.format("%s-%s.jar", artifactId.getName(), artifactId.getVersion().getVersion()));
    Files.copy(Locations.newInputSupplier(appJar), appJarFile);
    appJar.delete();

    try {
      applicationLifecycleService.deployAppAndArtifact(NamespaceId.DEFAULT, "appName", artifactId, appJarFile, null,
                                                       new ProgramTerminator() {
          @Override
          public void stop(ProgramId programId) throws Exception {
            // no-op
          }
        });
      Assert.fail("expected application deployment to fail.");
    } catch (Exception e) {
      // expected
    }

    // the artifact should have been cleaned up, and this should throw a not found exception
    artifactRepository.getArtifact(artifactId);
  }

  @Test
  public void testAppDeletionMultipleNS() throws Exception {

    // deploy same app in two namespaces
    deploy(WordCountApp.class, Constants.Gateway.API_VERSION_3_TOKEN, TEST_NAMESPACE1);
    deploy(WordCountApp.class);
    ProgramId wordcountFlow1 = new ProgramId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME, ProgramType.FLOW,
                                             WORDCOUNT_FLOW_NAME);
    ApplicationId defaultNSAppId = new ApplicationId(NamespaceId.DEFAULT.getNamespace(), WORDCOUNT_APP_NAME);
    ApplicationId testNSAppId = new ApplicationId(TEST_NAMESPACE1, WORDCOUNT_APP_NAME);

    // start a program in one namespace
    // flow is stopped initially
    Assert.assertEquals("STOPPED", getProgramStatus(wordcountFlow1.toId()));
    // start a flow and check the status
    startProgram(wordcountFlow1.toId());
    waitState(wordcountFlow1.toId(), ProgramRunStatus.RUNNING.toString());

    // delete the app from another (default namespace)
    deleteApp(defaultNSAppId.toId(), 200);

    // cleanup
    stopProgram(wordcountFlow1.toId());
    waitState(wordcountFlow1.toId(), "STOPPED");
    deleteApp(testNSAppId.toId(), 200);
  }
}
