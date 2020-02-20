/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.services.http.handlers;

import io.cdap.cdap.AllProgramsApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ApplicationMetadataHttpHandlerTest extends AppFabricTestBase {
  @Test
  public void testGetApplicationMetadata() throws Exception {
    String namespace = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;
    String version = ApplicationId.DEFAULT_VERSION;

    // Deploy the app
    deploy(AllProgramsApp.class, 200, Constants.Gateway.API_VERSION_3_TOKEN, namespace);

    // Fetch and verify application metadata
    List<ApplicationMeta> allMetadata = getAllApplicationMetadata(namespace);
    Assert.assertEquals(1, allMetadata.size());
    ApplicationMeta metadata = allMetadata.get(0);
    Assert.assertEquals(appName, metadata.getSpec().getName());

    ApplicationId appId = new ApplicationId(namespace, appName, version);
    ApplicationMeta metadata2 = getApplicationMetadata(appId);
    Assert.assertEquals(metadata.toString(), metadata2.toString());

    // Delete the app
    HttpResponse response = doDelete(getVersionedAPIPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace));
    Assert.assertEquals(200, response.getResponseCode());
  }

  @Test
  public void testGetAllApplicationMetadata() throws Exception {
    String namespace1 = TEST_NAMESPACE1;
    String appName = AllProgramsApp.NAME;
    String version1 = VERSION1;

    // Deploy the artifact
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.from(namespace1), appName, "1.0.0-SNAPSHOT");
    HttpResponse response = addAppArtifact(artifactId, AllProgramsApp.class);
    Assert.assertEquals(200, response.getResponseCode());

    // Deploy app1 v1
    ApplicationId app1V1Id = new ApplicationId(namespace1, appName + "-1", version1);
    deploy(app1V1Id, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));

    List<ApplicationMeta> metadataList;
    ApplicationMeta metadata;

    // Verify get all application metadata call returns 1 metadata
    metadataList = getAllApplicationMetadata(namespace1);
    Assert.assertEquals(1, metadataList.size());
    metadata = metadataList.get(0);
    Assert.assertEquals(app1V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(version1, metadata.getSpec().getAppVersion());

    // Deploy app1 v2
    ApplicationId app1V2Id = new ApplicationId(namespace1, appName + "-1", VERSION2);
    deploy(app1V2Id, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));

    // Verify get all application metadata call returns both.
    metadataList = getAllApplicationMetadata(namespace1);
    Assert.assertEquals(2, metadataList.size());
    metadata = metadataList.get(0);
    Assert.assertEquals(app1V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(version1, metadata.getSpec().getAppVersion());
    metadata = metadataList.get(1);
    Assert.assertEquals(app1V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(VERSION2, metadata.getSpec().getAppVersion());

    // Deploy app2 v1
    ApplicationId app2V1Id = new ApplicationId(namespace1, appName + "-2", version1);
    deploy(app2V1Id, new AppRequest<>(ArtifactSummary.from(artifactId.toArtifactId())));

    // Verify get all application metadata call returns 3 metadata.
    metadataList = getAllApplicationMetadata(namespace1);
    Assert.assertEquals(3, metadataList.size());
    metadata = metadataList.get(0);
    Assert.assertEquals(app1V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(version1, metadata.getSpec().getAppVersion());
    metadata = metadataList.get(1);
    Assert.assertEquals(app1V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(VERSION2, metadata.getSpec().getAppVersion());
    metadata = metadataList.get(2);
    Assert.assertEquals(app2V1Id.getApplication(), metadata.getSpec().getName());
    Assert.assertEquals(version1, metadata.getSpec().getAppVersion());

    // Cleanup
    HttpResponse resp;
    resp = doDelete(getVersionedAPIPath("apps/",
            Constants.Gateway.API_VERSION_3_TOKEN, namespace1));
    Assert.assertEquals(200, response.getResponseCode());
  }

}
