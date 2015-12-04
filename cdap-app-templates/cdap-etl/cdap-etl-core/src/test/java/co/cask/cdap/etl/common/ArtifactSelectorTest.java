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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.artifact.ArtifactId;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginPropertyField;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 */
public class ArtifactSelectorTest {

  @Test
  public void testSelection() {
    SortedMap<ArtifactId, PluginClass> plugins = new TreeMap<>();
    // doesn't matter what this is, since we only select on artifact id.
    PluginClass pluginClass = new PluginClass("type", "name", "desc", "com.company.class", "field",
                                              ImmutableMap.<String, PluginPropertyField>of());

    // put every combination of abc or def as name, 1.0.0 or 2.0.0 as version, and system or user as scope
    plugins.put(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM), pluginClass);
    plugins.put(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM), pluginClass);
    plugins.put(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER), pluginClass);
    plugins.put(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER), pluginClass);
    plugins.put(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM), pluginClass);
    plugins.put(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM), pluginClass);
    plugins.put(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER), pluginClass);
    plugins.put(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER), pluginClass);

    // test scope only
    ArtifactSelector selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, null, null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, null, null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());

    // test name only
    selector = new ArtifactSelector("type", "name", null, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", null, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", null, "xyz", null);
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }

    // test version only
    selector = new ArtifactSelector("type", "name", null, null, new ArtifactVersion("1.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", null, null, new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", null, null, new ArtifactVersion("3.0.0"));
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }

    // test name + version
    selector = new ArtifactSelector("type", "name", null, "abc", new ArtifactVersion("1.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", null, "abc", new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", null, "def", new ArtifactVersion("1.0.0"));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", null, "def", new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", null, "xyz", new ArtifactVersion("1.0.0"));
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }
    try {
      selector = new ArtifactSelector("type", "name", null, "abc", new ArtifactVersion("3.0.0"));
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }

    // test name + scope
    selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, "xyz", null);
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }

    // test version + scope
    selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, null, new ArtifactVersion("1.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, null, new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, null, new ArtifactVersion("1.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, null, new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", ArtifactScope.SYSTEM, "xyz", null);
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }

    // test name + version + scope
    selector = new ArtifactSelector("type", "name", ArtifactScope.USER, "def", new ArtifactVersion("2.0.0"));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    try {
      selector = new ArtifactSelector("type", "name", ArtifactScope.USER, "xyz", new ArtifactVersion("2.0.0"));
      selector.select(plugins);
    } catch (Exception e) {
      // expected
    }
  }
}
