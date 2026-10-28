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
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepository;
import io.cdap.cdap.internal.app.runtime.artifact.RemoteArtifactRepositoryReader;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.File;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.jar.Manifest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

public class ArtifactHttpHandlerInternalTest extends ArtifactHttpHandlerTestBase {
  private static final ArtifactId PARENT_ARTIFACT_ID =
      NamespaceId.SYSTEM.artifact("sysApp", "1.0.0");
  private static final PluginClass TEST_PLUGIN_CLASS = PluginClass.builder()
      .setType("my type")
      .setName("my plugin")
      .setDescription("plugin description")
      .setClassName(AllProgramsApp.class.getName())
      .setRequirements(new Requirements(Collections.singleton("reqDataset")))
      .build();

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
    ArtifactId systemArtifactId =
        NamespaceId.SYSTEM.artifact(systemArtfiactName, systemArtifactVeresion);
    addAppAsSystemArtifacts(systemArtifactId);

    // Fetch ArtifactDetail using RemoteArtifactRepositoryReader and verify
    ArtifactRepositoryReader remoteReader =
        getInjector().getInstance(RemoteArtifactRepositoryReader.class);
    ArtifactDetail detail = remoteReader.getArtifact(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertNotNull(detail);
    ArtifactDetail expectedDetail =
        getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertEquals(detail, expectedDetail);

    wipeData();
  }

  @Test
  public void testRemoteArtifactRepositoryReaderGetArtifactWithRetries() throws Exception {
    // Add a system artifact
    String systemArtfiactName = "sysApp";
    String systemArtifactVeresion = "1.0.0";
    ArtifactId systemArtifactId =
        NamespaceId.SYSTEM.artifact(systemArtfiactName, systemArtifactVeresion);
    addAppAsSystemArtifacts(systemArtifactId);
    // Fetch ArtifactDetail using RemoteArtifactRepositoryReader and verify
    RemoteArtifactRepositoryReader remoteReader =
        getInjector().getInstance(RemoteArtifactRepositoryReader.class);
    RemoteClient remoteClient = remoteReader.getRemoteClient();

    RemoteClient mockRemoteClient = Mockito.spy(remoteClient);
    RemoteArtifactRepositoryReader mockRemoteReader = Mockito.spy(remoteReader);
    Mockito.doReturn(mockRemoteClient).when(mockRemoteReader).getRemoteClient();

    Mockito
        .doThrow(new RetryableException(new SocketTimeoutException()))
        .doAnswer(InvocationOnMock::callRealMethod)
        .when(mockRemoteClient).execute(Mockito.any());

    ArtifactDetail detail =
        mockRemoteReader.getArtifact(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertNotNull(detail);
    ArtifactDetail expectedDetail =
        getArtifactDetailFromRepository(Id.Artifact.fromEntityId(systemArtifactId));
    Assert.assertEquals(detail, expectedDetail);
    Mockito.verify(mockRemoteClient, Mockito.times(2)).execute(Mockito.any());

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

  private RemoteArtifactRepository createRemoteArtifactRepository() {
    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    RemoteArtifactRepositoryReader remoteReader =
        getInjector().getInstance(RemoteArtifactRepositoryReader.class);
    return new RemoteArtifactRepository(cConf, remoteReader);
  }

  private RemoteArtifactRepository setupRemoteArtifactRepositoryWithPlugins() throws Exception {
    addAppAsSystemArtifacts(PARENT_ARTIFACT_ID);

    CConfiguration cConf = getInjector().getInstance(CConfiguration.class);
    ArtifactRepository repo = getInjector().getInstance(ArtifactRepository.class);
    File localDataDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR));
    File namespaceBase = new File(
        new File(localDataDir, cConf.get(Constants.Namespace.NAMESPACES_DIR)),
        NamespaceId.DEFAULT.getNamespace());
    namespaceBase.mkdirs();

    Set<ArtifactRange> parentRanges = Collections.singleton(
        new ArtifactRange(
            NamespaceId.SYSTEM.getNamespace(),
            PARENT_ARTIFACT_ID.getArtifact(),
            new ArtifactVersion("1.0.0"),
            true,
            new ArtifactVersion("2.0.0"),
            false));

    for (String version : new String[]{"1.0.0", "2.0.0"}) {
      File jarFile = new File(namespaceBase, "myPluginArtifact-" + version + ".jar");
      buildAppArtifact(AllProgramsApp.class, new Manifest(), jarFile);
      try {
        repo.addArtifact(
            new Id.Artifact(
                Id.Namespace.fromEntityId(NamespaceId.DEFAULT),
                "myPluginArtifact",
                new ArtifactVersion(version)),
            jarFile,
            parentRanges,
            Collections.singleton(TEST_PLUGIN_CLASS));
      } finally {
        jarFile.delete();
      }
    }

    return createRemoteArtifactRepository();
  }

  @Test
  public void testRemoteArtifactRepositoryGetPluginsAscOrder() throws Exception {
    RemoteArtifactRepository remoteRepo = setupRemoteArtifactRepositoryWithPlugins();

    SortedMap<ArtifactDescriptor, PluginClass> ascPlugins = remoteRepo.getPlugins(
        NamespaceId.DEFAULT,
        Id.Artifact.fromEntityId(PARENT_ARTIFACT_ID),
        "my type",
        "my plugin",
        id -> true,
        10,
        ArtifactSortOrder.ASC);
    Assert.assertEquals(2, ascPlugins.size());
    Assert.assertEquals("1.0.0", ascPlugins.firstKey().getArtifactId().getVersion().getVersion());
    Assert.assertEquals("2.0.0", ascPlugins.lastKey().getArtifactId().getVersion().getVersion());
    Assert.assertNotNull(ascPlugins.firstKey().getLocation());
    Assert.assertNotNull(ascPlugins.lastKey().getLocation());
    PluginClass resolvedPlugin = ascPlugins.get(ascPlugins.firstKey());
    Assert.assertEquals(TEST_PLUGIN_CLASS, resolvedPlugin);
    Assert.assertEquals(
        Collections.singleton("reqdataset"), resolvedPlugin.getRequirements().getDatasetTypes());
  }

  @Test
  public void testRemoteArtifactRepositoryGetPluginsDescWithLimit() throws Exception {
    RemoteArtifactRepository remoteRepo = setupRemoteArtifactRepositoryWithPlugins();

    SortedMap<ArtifactDescriptor, PluginClass> descLimited = remoteRepo.getPlugins(
        NamespaceId.DEFAULT,
        Id.Artifact.fromEntityId(PARENT_ARTIFACT_ID),
        "my type",
        "my plugin",
        id -> true,
        1,
        ArtifactSortOrder.DESC);
    Assert.assertEquals(1, descLimited.size());
    Assert.assertEquals("2.0.0", descLimited.firstKey().getArtifactId().getVersion().getVersion());
  }

  @Test
  public void testRemoteArtifactRepositoryGetPluginsWithPredicate() throws Exception {
    RemoteArtifactRepository remoteRepo = setupRemoteArtifactRepositoryWithPlugins();

    SortedMap<ArtifactDescriptor, PluginClass> filtered = remoteRepo.getPlugins(
        NamespaceId.DEFAULT,
        Id.Artifact.fromEntityId(PARENT_ARTIFACT_ID),
        "my type",
        "my plugin",
        id -> "1.0.0".equals(id.getVersion()),
        10,
        ArtifactSortOrder.ASC);
    Assert.assertEquals(1, filtered.size());
    Assert.assertEquals("1.0.0", filtered.firstKey().getArtifactId().getVersion().getVersion());
  }

  @Test(expected = PluginNotExistsException.class)
  public void testRemoteArtifactRepositoryGetPluginsNotFound() throws Exception {
    RemoteArtifactRepository remoteRepo = setupRemoteArtifactRepositoryWithPlugins();

    remoteRepo.getPlugins(
        NamespaceId.DEFAULT,
        Id.Artifact.fromEntityId(PARENT_ARTIFACT_ID),
        "my type",
        "nonExistentPlugin",
        id -> true,
        10,
        ArtifactSortOrder.ASC);
  }

  @Test(expected = ArtifactNotFoundException.class)
  public void testRemoteArtifactRepositoryGetPluginsParentArtifactNotFound() throws Exception {
    RemoteArtifactRepository remoteRepo = createRemoteArtifactRepository();
    ArtifactId missingParent = NamespaceId.SYSTEM.artifact("missingParent", "1.0.0");

    remoteRepo.getPlugins(
        NamespaceId.DEFAULT,
        Id.Artifact.fromEntityId(missingParent),
        "my type",
        "my plugin",
        id -> true,
        10,
        ArtifactSortOrder.ASC);
  }
}
