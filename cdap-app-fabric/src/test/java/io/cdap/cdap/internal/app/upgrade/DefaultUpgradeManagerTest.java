/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.upgrade;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.store.ScanApplicationsRequest;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.app.upgrade.UpgradeManager;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.PluginId;
import io.cdap.cdap.proto.upgrade.ApplicationUpgradeDetail;
import io.cdap.cdap.proto.upgrade.ArtifactUpgradeDetail;
import io.cdap.cdap.proto.upgrade.PluginUpgradeDetail;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class DefaultUpgradeManagerTest {

  @Mock
  private ApplicationPluginMappingFetcher mappingFetcher;
  @Mock
  private ArtifactRepository artifactRepository;
  @Mock
  private Store store;

  private UpgradeManager upgradeManager;

  @Before
  public void setUp() {
    this.upgradeManager = new DefaultUpgradeManager(mappingFetcher, artifactRepository, store);
  }

  /**
   * The following is the test data setup.
   * <p>
   * The test creates the following pipelines(including artifact and plugin details) in the format
   *      pipeline_name -> artifact_id(version) ->
   *      plugin1(artifact:version,type), plugin2(artifact,version,type)
   * Following pipelines are created:
   *     pipeline_1 -> cdap-data-pipeline(6.11.0) -> GCS(google-cloud,0.24.0,batchsource)
   *                                                ,trash(trash-plugin,1.2.0,batchsink)
   *     pipeline_2 -> cdap-data-pipeline(6.11.0) -> GCS(google-cloud,0.23.0,batchsource)
   *     pipeline_3 -> cdap-data-pipeline(6.10.0) -> sap(sap-plugin,1.10.0,batchsink)
   * </p>
   * <p>
   * Following application artifacts are present in the system:
   *      cdap-data-pipeline -> 6.11.0(latest) and 6.10.0
   * </p>
   * <p>
   * Following plugins are present in the system
   *      gcs -> google-cloud -> 0.23.0 and 0.24.0(latest)
   *      trash -> trash-plugin -> 1.1.0 and 1.2.0(latest)
   *      sap -> sap-plugin ->1.10.0(latest)
   * </p>
   */
  @Test
  public void testListUpgrades() throws Exception {
    when(mappingFetcher.fetchApplicationPluginMapping(NamespaceId.DEFAULT)).thenReturn(
        createAppPluginMappings());
    when(artifactRepository.getArtifactSummaries(NamespaceId.DEFAULT, true)).thenReturn(
        createArtifactSummaries());

    ApplicationSpecification appSpec1 = mock(ApplicationSpecification.class);
    when(appSpec1.getArtifactId()).thenReturn(createArtifactId("6.11.0"));
    ApplicationSpecification appSpec2 = mock(ApplicationSpecification.class);
    when(appSpec2.getArtifactId()).thenReturn(createArtifactId("6.11.0"));
    ApplicationSpecification appSpec3 = mock(ApplicationSpecification.class);
    when(appSpec3.getArtifactId()).thenReturn(createArtifactId("6.10.0"));

    Map<ApplicationId, ApplicationMeta> appsInStore = ImmutableMap.of(
        new ApplicationId("default", "pipeline_1"),
        new ApplicationMeta("pipeline_1", appSpec1, null),
        new ApplicationId("default", "pipeline_2"),
        new ApplicationMeta("pipeline_2", appSpec2, null),
        new ApplicationId("default", "pipeline_3"),
        new ApplicationMeta("pipeline_3", appSpec3, null)
    );
    doAnswer(invocation -> {
      BiConsumer<ApplicationId, ApplicationMeta> consumer = invocation.getArgument(2);
      appsInStore.forEach(consumer);
      return null;
    }).when(store)
        .scanApplications(any(ScanApplicationsRequest.class), anyInt(), any(BiConsumer.class));
    List<ApplicationUpgradeDetail> expected = createExpectedUpgradeDetails();

    List<ApplicationUpgradeDetail> actual = upgradeManager.listUpgrades(NamespaceId.DEFAULT);

    Assert.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    verify(store, times(1)).scanApplications(any(ScanApplicationsRequest.class), anyInt(),
        any(BiConsumer.class));
    verify(mappingFetcher, times(1)).fetchApplicationPluginMapping(NamespaceId.DEFAULT);
    verify(artifactRepository, times(1)).getArtifactSummaries(NamespaceId.DEFAULT, true);
  }

  @Test(expected = ApplicationNotFoundException.class)
  public void testListUpgradesThrowsException() throws Exception {
    when(mappingFetcher.fetchApplicationPluginMapping(NamespaceId.DEFAULT)).thenReturn(
        createAppPluginMappings());
    when(artifactRepository.getArtifactSummaries(NamespaceId.DEFAULT, true)).thenReturn(
        createArtifactSummaries());

    ApplicationSpecification appSpec1 = mock(ApplicationSpecification.class);
    when(appSpec1.getArtifactId()).thenReturn(createArtifactId("6.11.0"));
    ApplicationSpecification appSpec2 = mock(ApplicationSpecification.class);
    when(appSpec2.getArtifactId()).thenReturn(createArtifactId("6.11.0"));

    // No application artifact mapping is present for pipeline_3 and should hence throw an
    // exception.
    Map<ApplicationId, ApplicationMeta> appsInStore = ImmutableMap.of(
        new ApplicationId("default", "pipeline_1"),
        new ApplicationMeta("pipeline_1", appSpec1, null),
        new ApplicationId("default", "pipeline_2"),
        new ApplicationMeta("pipeline_2", appSpec2, null)
    );
    doAnswer(invocation -> {
      BiConsumer<ApplicationId, ApplicationMeta> consumer = invocation.getArgument(2);
      appsInStore.forEach(consumer);
      return null;
    }).when(store)
        .scanApplications(any(ScanApplicationsRequest.class), anyInt(), any(BiConsumer.class));

    upgradeManager.listUpgrades(NamespaceId.DEFAULT);
  }


  private ArtifactId createArtifactId(String version) {
    return new ArtifactId("cdap-data-pipeline", new ArtifactVersion(version), ArtifactScope.SYSTEM);
  }

  private ApplicationPluginMapping getAppPluginMapping(String appName, String pluginNamespace,
      String artifactId, String version, String pluginName, String type) {
    return new ApplicationPluginMapping(new ApplicationId("default", appName),
        new PluginId(pluginNamespace, artifactId, version, pluginName, type));
  }

  private List<ApplicationPluginMapping> createAppPluginMappings() {
    return ImmutableList.of(
        getAppPluginMapping("pipeline_1", "system", "google-cloud", "0.24.0", "GCS", "batchsource"),
        getAppPluginMapping("pipeline_1", "default", "trash-plugin", "1.2.0", "trash", "batchsink"),
        getAppPluginMapping("pipeline_2", "system", "google-cloud", "0.23.0", "GCS", "batchsource"),
        getAppPluginMapping("pipeline_3", "default", "sap-plugin", "1.10.0", "sap", "batchsink")
    );
  }

  private List<ArtifactSummary> createArtifactSummaries() {
    return ImmutableList.of(
        new ArtifactSummary("google-cloud", "0.23.0", ArtifactScope.SYSTEM),
        new ArtifactSummary("google-cloud", "0.24.0", ArtifactScope.SYSTEM),
        new ArtifactSummary("trash-plugin", "1.2.0", ArtifactScope.USER),
        new ArtifactSummary("trash-plugin", "1.1.0", ArtifactScope.USER),
        new ArtifactSummary("sap-plugin", "1.10.0", ArtifactScope.USER),
        new ArtifactSummary("cdap-data-pipeline", "6.10.0", ArtifactScope.SYSTEM),
        new ArtifactSummary("cdap-data-pipeline", "6.11.0", ArtifactScope.SYSTEM)
    );
  }

  private List<ApplicationUpgradeDetail> createExpectedUpgradeDetails() {
    return ImmutableList.of(
        // Pipeline 1 has all the latest versions and is does not need to be upgraded.
        new ApplicationUpgradeDetail("pipeline_1",
            new ArtifactUpgradeDetail("cdap-data-pipeline", "6.11.0", "6.11.0"),
            ImmutableList.of(
                new PluginUpgradeDetail(
                    new ArtifactUpgradeDetail("google-cloud", "0.24.0", "0.24.0"),
                    "GCS", "batchsource"),
                new PluginUpgradeDetail(new ArtifactUpgradeDetail("trash-plugin", "1.2.0", "1.2.0"),
                    "trash", "batchsink"))),
        // Pipeline 2 has all the latest application artifact but an older gcs version and is hence
        // upgrade eligible.
        new ApplicationUpgradeDetail("pipeline_2",
            new ArtifactUpgradeDetail("cdap-data-pipeline", "6.11.0", "6.11.0"),
            ImmutableList.of(
                new PluginUpgradeDetail(
                    new ArtifactUpgradeDetail("google-cloud", "0.23.0", "0.24.0"),
                    "GCS", "batchsource"))),
        // Pipeline 3 has the latest plugin version but older application artifact and is hence
        // upgrade eligible.
        new ApplicationUpgradeDetail("pipeline_3",
            new ArtifactUpgradeDetail("cdap-data-pipeline", "6.10.0", "6.11.0"),
            ImmutableList.of(
                new PluginUpgradeDetail(new ArtifactUpgradeDetail("sap-plugin", "1.10.0", "1.10.0"),
                    "sap", "batchsink")))
    );
  }
}