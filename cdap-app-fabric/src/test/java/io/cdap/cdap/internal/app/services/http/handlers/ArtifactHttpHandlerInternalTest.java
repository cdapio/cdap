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
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
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

  /**
   * Test {@link RemoteArtifactRepositoryReader#getArtifact}
   */
  @Test
  public void testRemoteArtifactRepositoryReaderGetArtifact() throws Exception {
    // Add a system artifact
    String systemArtfiactName = "sysApp";
    String systemArtifactVeresion = "1.0.0";
    ArtifactId systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, systemArtifactVeresion);
    addAppAsSystemArtifacts(systemArtifactId);

    // Fetch ArtifactDetail using RemoteArtifactRepositoryReader and verify
    ArtifactRepositoryReader remoteReader = getInjector().getInstance(RemoteArtifactRepositoryReader.class);
    ArtifactDetail detail = remoteReader.getArtifact(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertNotNull(detail);
    ArtifactDetail expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertTrue(detail.equals(expectedDetail));

    wipeData();
  }

  /**
   * Test {@link RemoteArtifactRepositoryReader#getArtifactDetails}
   */
  @Test
  public void testRemoteArtifactRepositoryReaderGetArtifactDetails() throws Exception {
    // Add an artifact with a number of versions
    String systemArtfiactName = "sysApp3";
    final int numVersions = 10;
    List<ArtifactId> artifactIds = new ArrayList<>();
    for (int i = 1; i <= numVersions; i++) {
      String version = String.format("%d.0.0", i);
      ArtifactId systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, version);
      addAppAsSystemArtifacts(systemArtifactId);
      artifactIds.add(systemArtifactId);
    }
    ArtifactRepositoryReader remoteReader = getInjector().getInstance(RemoteArtifactRepositoryReader.class);

    ArtifactRange range = null;
    List<ArtifactDetail> details = null;
    ArtifactId systemArtifactId = null;
    ArtifactDetail expectedDetail = null;
    int numLimits = 0;


    range = new ArtifactRange(NamespaceId.SYSTEM.getNamespace(),
                                            systemArtfiactName,
                                            new ArtifactVersion("1.0.0"),
                                            new ArtifactVersion(String.format("%d.0.0", numVersions)));
    // Fetch artifacts with the version in range [1.0.0, numVersions.0.0], but limit == 1 in desc order
    numLimits = 1;
    details = remoteReader.getArtifactDetails(range, numLimits, ArtifactSortOrder.DESC);
    Assert.assertEquals(numLimits, details.size());
    systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, String.format("%d.0.0", numVersions));
    expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertTrue(details.get(numLimits - 1).equals(expectedDetail));

    // Fetch artifacts with the version in range [1.0.0, numVersions.0.0], but limit == 3 in desc order
    numLimits = 3;
    details = remoteReader.getArtifactDetails(range, numLimits, ArtifactSortOrder.DESC);
    Assert.assertEquals(numLimits, details.size());
    for (int i = 0; i < numLimits; i++) {
      systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, String.format("%d.0.0", numVersions - i));
      expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
      Assert.assertTrue(details.get(i).equals(expectedDetail));
    }

    // Fetch artifacts with the version in range [1.0.0, numVersions.0.0], but limit == 1 in asec order
    details = remoteReader.getArtifactDetails(range, 1, ArtifactSortOrder.ASC);
    Assert.assertEquals(1, details.size());
    systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, "1.0.0");
    expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertTrue(details.get(0).equals(expectedDetail));

    // Fetch artifacts with the version in range [1.0.0, numVersions.0.0], but limit == 5 in asec order
    numLimits = 5;
    details = remoteReader.getArtifactDetails(range, numLimits, ArtifactSortOrder.ASC);
    Assert.assertEquals(numLimits, details.size());
    for (int i = 0; i < numLimits; i++) {
      systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, String.format("%d.0.0", i + 1));
      expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
      Assert.assertTrue(details.get(i).equals(expectedDetail));
    }

    int versionLow = 1;
    int versionHigh = numVersions / 2;
    range = new ArtifactRange(NamespaceId.SYSTEM.getNamespace(),
                                            systemArtfiactName,
                                            new ArtifactVersion(String.format("%d.0.0", versionLow)),
                                            new ArtifactVersion(String.format("%d.0.0", versionHigh)));
    // Fetch artifacts with the version in range [1.0.0, <numVersions/2>.0.0], but limit == numVersions in desc order
    numLimits = numVersions;
    details = remoteReader.getArtifactDetails(range, numLimits, ArtifactSortOrder.DESC);
    Assert.assertEquals(versionHigh - versionLow + 1, details.size());
    for (int i = 0; i < versionHigh - versionLow + 1; i++) {
      systemArtifactId = NamespaceId.SYSTEM.artifact(systemArtfiactName, String.format("%d.0.0", versionHigh - i));
      expectedDetail = getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
      Assert.assertTrue(details.get(i).equals(expectedDetail));
    }
  }
}
