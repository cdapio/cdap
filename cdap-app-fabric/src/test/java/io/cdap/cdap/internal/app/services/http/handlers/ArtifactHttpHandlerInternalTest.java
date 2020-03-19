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

import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class ArtifactHttpHandlerInternalTest extends ArtifactHttpHandlerTestBase {
  @Test
  public void testListArtifacts() throws Exception {
    List<ArtifactInfo> artifactInfoList = null;
    ArtifactInfo artifactInfo = null;

    // Add a system artifact
    String systemArtfiactName = "sysApp";
    String systemArtifactVeresion = "1.0.0";
    ArtifactId systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, systemArtifactVeresion);
    addAppAsSystemArtifacts(systemArtifactId);

    // Listing all artifacts in default namespace, should include system artifacts
    artifactInfoList = listArtifactsInternal(NamespaceId.DEFAULT.getNamespace());
    Assert.assertEquals(1, artifactInfoList.size());
    artifactInfo = artifactInfoList.stream().filter(info -> info.getScope().equals(ArtifactScope.SYSTEM))
      .findAny().orElse(null);
    Assert.assertNotNull(artifactInfo);
    Assert.assertEquals(ArtifactScope.SYSTEM, artifactInfo.getScope());
    Assert.assertEquals(systemArtfiactName, artifactInfo.getName());
    Assert.assertEquals(systemArtifactVeresion, artifactInfo.getVersion());
    Assert.assertTrue(artifactInfo.getClasses().getApps().size() > 0);

    // Add a user artifact, in addition to the above system artifact
    String userArtifactName = "userApp";
    String userArtifactVersion = "2.0.0";
    ArtifactId userArtifactId = NamespaceId.DEFAULT.artifact(userArtifactName, userArtifactVersion);
    addAppAsUserArtifacts(userArtifactId);

    // Listing all artifacts in default namespace, should include both user and system artifacts
    artifactInfoList = listArtifactsInternal(NamespaceId.DEFAULT.getNamespace());
    Assert.assertEquals(2, artifactInfoList.size());

    artifactInfo = artifactInfoList.stream().filter(info -> info.getScope().equals(ArtifactScope.USER))
      .findAny().orElse(null);
    Assert.assertNotNull(artifactInfo);
    Assert.assertEquals(ArtifactScope.USER, artifactInfo.getScope());
    Assert.assertEquals(userArtifactName, artifactInfo.getName());
    Assert.assertEquals(userArtifactVersion, artifactInfo.getVersion());
    Assert.assertTrue(artifactInfo.getClasses().getApps().size() > 0);

    artifactInfo = artifactInfoList.stream().filter(info -> info.getScope().equals(ArtifactScope.SYSTEM))
      .findAny().orElse(null);
    Assert.assertNotNull(artifactInfo);
    Assert.assertEquals(ArtifactScope.SYSTEM, artifactInfo.getScope());
    Assert.assertEquals(systemArtfiactName, artifactInfo.getName());
    Assert.assertEquals(systemArtifactVeresion, artifactInfo.getVersion());
    Assert.assertTrue(artifactInfo.getClasses().getApps().size() > 0);
  }

  @Test
  public void testGetArtifactLocation() throws Exception {
    // Add a system artifact
    String systemArtfiactName = "sysApp";
    String systemArtifactVeresion = "1.0.0";
    ArtifactId systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, systemArtifactVeresion);
    addAppAsSystemArtifacts(systemArtifactId);

    String locationPath = getArtifactLocationPath(systemArtifactId);
    Assert.assertNotNull(locationPath);

    ArtifactDetail expectedArtifactDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertEquals(expectedArtifactDetail.getDescriptor().getLocation().toURI().getPath(),
                        locationPath);
  }
}
