/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.app.UpdateSourceControlMetaRequest;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.common.http.HttpResponse;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests for {@link RemoteApplicationManager} and {@link LocalApplicationManager}
 */
@RunWith(Parameterized.class)
public class ApplicationManagerTest extends AppFabricTestBase {

  private enum ApplicationManagerType {
    LOCAL,
    REMOTE,
  }

  private final ApplicationManagerType managerType;

  private static final String namespace = TEST_NAMESPACE1;
  private static final Id.Artifact artifact = Id.Artifact.from(new Id.Namespace(namespace),
      AllProgramsApp.class.getSimpleName(), "1.0.0-SNAPSHOT");
  private static final AppRequest<?> request = new AppRequest<>(
      ArtifactSummary.from(artifact.toArtifactId()));

  public ApplicationManagerTest(ApplicationManagerType type) {
    this.managerType = type;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
        {ApplicationManagerType.LOCAL},
        {ApplicationManagerType.REMOTE},
    });
  }

  @SuppressWarnings("checkstyle:MissingSwitchDefault")
  private ApplicationManager getApplicationManager(ApplicationManagerType type) {
    if (type == ApplicationManagerType.LOCAL) {
      return AppFabricTestBase.getInjector().getInstance(LocalApplicationManager.class);
    }
    return AppFabricTestBase.getInjector().getInstance(RemoteApplicationManager.class);
  }


  @Before
  public void setUp() throws Exception {
    HttpResponse response = addAppArtifact(artifact, AllProgramsApp.class);
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testDeployApp() throws Exception {
    ApplicationManager manager = getApplicationManager(managerType);
    ApplicationReference appRef = new ApplicationReference(namespace, AllProgramsApp.NAME);

    // Deploy the application
    ApplicationId deployedAppId = manager.deployApp(
        appRef,
        new PullAppResponse<>(AllProgramsApp.NAME, "originalHash", request)
    );

    // fetch and validate the application version is created
    HttpResponse response = doGet(getVersionedApiPath(
        String.format("apps/%s/versions/%s", deployedAppId.getApplication(),
            deployedAppId.getVersion()),
        Constants.Gateway.API_VERSION_3_TOKEN, namespace));

    Assert.assertEquals(200, response.getResponseCode());

    ApplicationDetail detail = GSON.fromJson(response.getResponseBodyAsString(),
        ApplicationDetail.class);
    Assert.assertEquals(false, detail.getChange().getLatest());

    // Delete the application
    Assert.assertEquals(
        200,
        doDelete(getVersionedApiPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test
  public void testMarkAppVersionsLatest() throws Exception {
    ApplicationManager manager = getApplicationManager(managerType);
    ApplicationReference appRef = new ApplicationReference(namespace, AllProgramsApp.NAME);

    // Deploy the application
    ApplicationId deployedAppId = manager.deployApp(
        appRef,
        new PullAppResponse<>(AllProgramsApp.NAME, "originalHash", request)
    );

    // mark the application as latest
    manager.markAppVersionsLatest(new NamespaceId(namespace), Collections.singletonList(
        new AppVersion(deployedAppId.getApplication(), deployedAppId.getVersion())));

    // fetch and validate the application version is created
    // fetch and validate the application version is created
    HttpResponse response = doGet(getVersionedApiPath(
        String.format("apps/%s/versions/%s", deployedAppId.getApplication(),
            deployedAppId.getVersion()),
        Constants.Gateway.API_VERSION_3_TOKEN, namespace));

    Assert.assertEquals(200, response.getResponseCode());

    ApplicationDetail detail = GSON.fromJson(response.getResponseBodyAsString(),
        ApplicationDetail.class);
    Assert.assertEquals(true, detail.getChange().getLatest());

    // Delete the application
    Assert.assertEquals(
        200,
        doDelete(getVersionedApiPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }

  @Test
  public void testUpdateSourceControlMeta() throws Exception {
    ApplicationManager manager = getApplicationManager(managerType);
    ApplicationReference appRef = new ApplicationReference(namespace, AllProgramsApp.NAME);

    // Deploy the application
    ApplicationId deployedAppId = manager.deployApp(
        appRef,
        new PullAppResponse<>(AllProgramsApp.NAME, "originalHash", request)
    );

    UpdateMultiSourceControlMetaReqeust request = new UpdateMultiSourceControlMetaReqeust(
        Collections.singletonList(new UpdateSourceControlMetaRequest(
            deployedAppId.getApplication(),
            deployedAppId.getVersion(),
            "updatedHash"
        ))
    );

    // update the source control meta
    manager.updateSourceControlMeta(new NamespaceId(namespace), request);

    // fetch and validate the application version is created
    // fetch and validate the application version is created
    HttpResponse response = doGet(getVersionedApiPath(
        String.format("apps/%s/versions/%s", deployedAppId.getApplication(),
            deployedAppId.getVersion()),
        Constants.Gateway.API_VERSION_3_TOKEN, namespace));

    Assert.assertEquals(200, response.getResponseCode());

    ApplicationDetail detail = GSON.fromJson(response.getResponseBodyAsString(),
        ApplicationDetail.class);
    Assert.assertEquals("updatedHash", detail.getSourceControlMeta().getFileHash());

    // Delete the application
    Assert.assertEquals(
        200,
        doDelete(getVersionedApiPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace)).getResponseCode());
  }
}
