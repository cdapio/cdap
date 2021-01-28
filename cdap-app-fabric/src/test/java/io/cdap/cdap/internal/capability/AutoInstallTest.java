/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.capability;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.capability.autoinstall.HubPackage;
import io.cdap.cdap.internal.capability.autoinstall.Spec;
import io.cdap.cdap.proto.artifact.ArtifactRanges;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Tests for Auto install
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Files.class, HubPackage.class, HttpClients.class})
public class AutoInstallTest {
  private static final Gson GSON = new Gson();

  @Test
  public void testAutoInstallPlugins() throws Exception {
    // Setup mocks
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, "/tmp");
    cConf.set(Constants.AppFabric.TEMP_DIR, "appfabric");
    cConf.setInt(Constants.Capability.AUTO_INSTALL_THREADS, 5);
    ArtifactRepository artifactRepository = PowerMockito.mock(ArtifactRepository.class);
    CapabilityApplier capabilityApplier = new CapabilityApplier(null, null,
                                                                null, null, null,
                                                                artifactRepository, null, cConf);
    CapabilityApplier ca = Mockito.spy(capabilityApplier);
    PowerMockito.mockStatic(HttpClients.class);
    PowerMockito.mockStatic(Files.class);
    PowerMockito.mockStatic(File.class);
    PowerMockito.mockStatic(Paths.class);
    PowerMockito.mockStatic(java.nio.file.Files.class);

    File mockFile = new File("/tmp/mock");
    Mockito.when(File.createTempFile(anyString(), anyString(), any()))
      .thenReturn(mockFile);
    Path mockPath = PowerMockito.mock(Path.class);
    Mockito.when(Paths.get(mockFile.getPath())).thenReturn(mockPath);

    URL packagesUrl = new URL("https://my.hub.io/v2/packages.json");
    HubPackage pkg1 = new HubPackage("my-plugin", "1.0.0", "My Plugin",
                                     "My Plugin", "Cask", "Cask", "[6.1.1,6.3.0]",
                                     1554766945, true, Collections.singletonList("hydrator-plugin"),
                                     false, null);
    HubPackage pkg2 = new HubPackage("my-plugin", "2.0.0", "My Plugin",
                                     "My Plugin", "Cask", "Cask", "[6.3.1,6.4.0]",
                                     1554766945, true, Collections.singletonList("hydrator-plugin"),
                                     false, null);
    HubPackage pkg3 = new HubPackage("my-plugin", "3.0.0", "My Plugin",
                                     "My Plugin", "Cask", "Cask",
                                     "[6.4.1,7.0.0-SNAPSHOT)", 1554766945,
                                     true, Collections.singletonList("hydrator-plugin"),
                                     false, null);
    String packagesJson = GSON.toJson(ImmutableList.of(pkg1, pkg2, pkg3));

    URL specUrl = new URL("https://my.hub.io/v2/packages/my-plugin/2.0.0/spec.json");
    List<Spec.Action.Argument> arguments = Arrays.asList(
      new Spec.Action.Argument("config", "my-plugin-2.0.0.json", false),
      new Spec.Action.Argument("jar", "my-plugin-2.0.0.jar", false),
      new Spec.Action.Argument("name", "my-plugin", false),
      new Spec.Action.Argument("version", "2.0.0", false));
    Spec.Action action = new Spec.Action("one_step_deploy_plugin", "Deploy my plugin", arguments);
    Spec spec = new Spec("1.0", "My Plugin", "My Plugin", "Cask", "Cask",
                         1554766945, "[6.3.1,6.4.0]",
                         Collections.singletonList("hydrator-plugin"), true, Collections.singletonList(action));
    String specJson = GSON.toJson(spec);

    URL configUrl = new URL("https://my.hub.io/v2/packages/my-plugin/2.0.0/my-plugin-2.0.0.json");
    Map<String, String> properties = ImmutableMap.of("key1", "value1", "key2", "value2");
    Map<String, Object> config = ImmutableMap.of("parents",
                                                 ImmutableList.of("system:cdap-data-pipeline[6.3.1,6.4.0]",
                                                                  "system:cdap-data-streams[6.3.1,6.4.0]"),
                                                 "properties",
                                                 properties);
    String configJson = GSON.toJson(config);
    // Mock http requests to hub
    Mockito.when(HttpClients.doGetAsString(packagesUrl)).thenReturn(packagesJson);
    Mockito.when(HttpClients.doGetAsString(specUrl)).thenReturn(specJson);
    Mockito.when(HttpClients.doGetAsString(configUrl)).thenReturn(configJson);

    // Set current CDAP version
    Mockito.doReturn("6.4.0").when(ca).getCurrentVersion();

    // Test plugin auto install
    ca.doStartup();
    ca.autoInstallResources("mycapability",
                            Collections.singletonList(new URL("https://my.hub.io")));
    ca.doShutdown();

    // Verify that the correct version of the plugin was installed
    Set<ArtifactRange> ranges = ImmutableSet.of(
      ArtifactRanges.parseArtifactRange("system:cdap-data-pipeline[6.3.1,6.4.0]"),
      ArtifactRanges.parseArtifactRange("system:cdap-data-streams[6.3.1,6.4.0]"));
    Id.Artifact artifact = Id.Artifact.from(Id.Namespace.DEFAULT, "my-plugin", "2.0.0");
    Mockito.verify(artifactRepository, Mockito.times(1)).addArtifact(artifact,
                                                   mockFile, ranges,
                                                   ImmutableSet.of());
    Mockito.verify(artifactRepository, Mockito.times(1)).writeArtifactProperties(artifact, properties);
    // Verify that temp file was deleted
    PowerMockito.verifyStatic();
    java.nio.file.Files.deleteIfExists(mockPath);
  }

  private <T> T readFile(String fileName, Class<T> type) throws FileNotFoundException {
    File testJson = new File(AutoInstallTest.class.getResource(String.format("/%s", fileName)).getPath());
    FileReader fileReader = new FileReader(testJson);
    return GSON.fromJson(fileReader, type);
  }

  @Test
  public void testAutoInstallConfig() throws Exception {
    CapabilityConfig capabilityConfig = readFile("cap2.json", CapabilityConfig.class);
    Assert.assertEquals(capabilityConfig.getHubs(), Collections.singletonList(new URL("https://my.hub.io")));
  }

  @Test
  public void testDeserializePackageJson() throws Exception {
    HubPackage[] deserializedHubPackages = readFile("packages.json", HubPackage[].class);
    HubPackage[] expectedHubPackages = {
      new HubPackage("plugin-1", "1.0.0", "Description1", "Label1",
                     "Author1", "Org1", "[6.3.0,7.0.0-SNAPSHOT)", 1473901763,
                     false, Arrays.asList("category1"), true, "https://www.foo.com"),
      new HubPackage("plugin-2", "2.0.0", "Description2", "Label2",
                     "Author2", "Org2", "[6.1.1,6.1.1]", 1510006834, true,
                     Arrays.asList("category2", "category3"), false, null)
    };
    Assert.assertArrayEquals(expectedHubPackages, deserializedHubPackages);
    Assert.assertTrue(deserializedHubPackages[0].versionIsInRange("6.4.0"));
    Assert.assertFalse(deserializedHubPackages[0].versionIsInRange("7.0.0"));
    Assert.assertTrue(deserializedHubPackages[1].versionIsInRange("6.1.1"));
    Assert.assertFalse(deserializedHubPackages[1].versionIsInRange("6.1.2"));
  }

  @Test
  public void testDeserializeSpecJson() throws Exception {
    Spec deserializedSpec = readFile("spec.json", Spec.class);
    List<Spec.Action.Argument> arguments = Arrays.asList(
      new Spec.Action.Argument("name", "my-plugin", false),
      new Spec.Action.Argument("version", "1.3.0", false),
      new Spec.Action.Argument("scope", "user", false),
      new Spec.Action.Argument("config", "my-plugins-1.3.0.json", false),
      new Spec.Action.Argument("jar", "my-plugins-1.3.0.jar", false));
    List<Spec.Action> actions = Collections.singletonList(
      new Spec.Action("one_step_deploy_plugin", "Label", arguments));
    Spec expectedSpec = new Spec("1.0", "Label", "Description", "Author",
                                 "Org", 1603825065, "[6.1.1,7.0.0-SNAPSHOT)",
                                 Collections.singletonList("hydrator-plugin"), true, actions);
    Assert.assertEquals(expectedSpec, deserializedSpec);
  }
}
