/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.plugin.PluginClass;
import org.junit.Assert;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 */
public class ArtifactSelectorTest {

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testSelection() {
    SortedMap<ArtifactId, PluginClass> plugins = new TreeMap<>();
    // doesn't matter what this is, since we only select on artifact id.
    PluginClass pluginClass =
      PluginClass.builder().setName("name").setType("type").setDescription("desc")
        .setClassName("com.company.class").setConfigFieldName("field").setProperties(ImmutableMap.of()).build();

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
    ArtifactSelector selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, null, null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());

    // test name only
    selector = new ArtifactSelector(null, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "xyz", null);
    Assert.assertNull(selector.select(plugins));

    // test version only
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("3.0.0"), true, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test range only
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, null, new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), false, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test name + version
    selector = new ArtifactSelector(null, "abc", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "abc", new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "def", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "def", new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "xyz", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertNull(selector.select(plugins));
    selector = new ArtifactSelector(null, "abc", new ArtifactVersionRange(
      new ArtifactVersion("3.0.0"), true, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test name + scope
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, "abc", null);
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, "def", null);
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "xyz", null);
    Assert.assertNull(selector.select(plugins));

    // test version + scope
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, null, new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("3.0.0"), true, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test name + range
    selector = new ArtifactSelector(null, "abc", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "abc", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(null, "def", new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), false, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test scope + range
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, null, new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, null, new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), false, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test name + version + scope
    selector = new ArtifactSelector(ArtifactScope.USER, "def", new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, "xyz", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0"), true, new ArtifactVersion("1.0.0"), true));
    Assert.assertNull(selector.select(plugins));

    // test name + scope + range
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "abc", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, "abc", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("abc", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "def", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), false));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("1.0.0"), ArtifactScope.SYSTEM),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.USER, "def", new ArtifactVersionRange(
      new ArtifactVersion("1.0.0-SNAPSHOT"), true, new ArtifactVersion("2.0.0"), true));
    Assert.assertEquals(new ArtifactId("def", new ArtifactVersion("2.0.0"), ArtifactScope.USER),
                        selector.select(plugins).getKey());
    selector = new ArtifactSelector(ArtifactScope.SYSTEM, "abc", new ArtifactVersionRange(
      new ArtifactVersion("2.0.0"), false, new ArtifactVersion("3.0.0"), true));
    Assert.assertNull(selector.select(plugins));
  }
}
