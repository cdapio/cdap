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

package io.cdap.cdap.internal.app.runtime.plugin;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.internal.app.runtime.ProgramClassLoader;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PluginInstantiatorTest {
  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  /**
   * If a plugin has some resources in main jar and in libraries, main jar should take precedence
   */
  @Test
  public void testResourceOrder() throws IOException {
    File appDir = TMP_FOLDER.newFolder();
    File pluginsDir = TMP_FOLDER.newFolder();
    File pluginDir = TMP_FOLDER.newFolder();
    File pluginArchive = TMP_FOLDER.newFile();
    File jarDir = TMP_FOLDER.newFolder();
    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, appDir, this.getClass().getClassLoader());
    PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf,
                                                                   programClassLoader,
                                                                   pluginsDir);
    FileUtils.write(new File(jarDir, "test.class"), "jarData");
    FileUtils.write(new File(pluginDir, "test.class"), "pluginData");
    BundleJarUtil.createJar(jarDir, new File(pluginDir, "library.jar"));
    BundleJarUtil.createJar(pluginDir, pluginArchive);
    pluginInstantiator.addArtifact(Locations.toLocation(pluginArchive), artifactId);
    PluginClassLoader loader = pluginInstantiator.getPluginClassLoader(artifactId, Collections.emptyList());
    Assert.assertEquals("pluginData", IOUtils.toString(loader.getResource("test.class")));
    pluginInstantiator.close();
  }

  @Test
  public void testSubstituteMacros() throws Exception {
    File appDir = TMP_FOLDER.newFolder();
    File pluginsDir = TMP_FOLDER.newFolder();
    ArtifactId artifactId = new ArtifactId("dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TMP_FOLDER.newFolder().getAbsolutePath());

    ProgramClassLoader programClassLoader = new ProgramClassLoader(cConf, appDir, this.getClass().getClassLoader());
    PluginInstantiator pluginInstantiator = new PluginInstantiator(cConf,
                                                                   programClassLoader,
                                                                   pluginsDir);

    PluginClass pluginClass = PluginClass.builder().setClassName("").setName("").setCategory("").setConfigFieldName("")
      .setRequirements(Requirements.EMPTY).setType("").setDescription("")
      .add("key1", new PluginPropertyField("key1", "", "string", false, true))
      .add("key2", new PluginPropertyField("key2", "", "Connection", false, true, false,
                                           ImmutableSet.of("child1", "child2")))
      .add("child1", new PluginPropertyField("child1", "", "string", false, true))
      .add("child2", new PluginPropertyField("child2", "", "string", false, true))
      .build();
    Gson gson = new Gson();

    // test macro with additional fields can be evaluated successfully
    Map<String, String> childProperties =
      ImmutableMap.of("child1", "childVal1", "child2", "${secure(acc)}", "child3", "val3");
    Map<String, String> properties = ImmutableMap.of("key2", gson.toJson(childProperties), "key1", "val1");

    Plugin plugin = new Plugin(Collections.emptyList(), artifactId, pluginClass,
                               PluginProperties.builder().addAll(properties).build());

    PluginProperties pluginProperties = pluginInstantiator.substituteMacros(plugin, null, null);
    Map<String, String> expectedChildProperties = new HashMap<>();
    expectedChildProperties.put("child1", "childVal1");
    expectedChildProperties.put("child2", null);
    Assert.assertEquals(PluginProperties.builder().addAll(
      ImmutableMap.of("key1", "val1", "key2", gson.toJson(expectedChildProperties))).build(), pluginProperties);
  }
}
