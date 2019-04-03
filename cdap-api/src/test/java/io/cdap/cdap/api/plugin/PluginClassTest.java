/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.api.plugin;


import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Test for {@link PluginClass}
 */
public class PluginClassTest {
  private static final Gson GSON = new Gson();

  @Test
  public void testOldFormat() {
    // test that old json representation which does not have a requirements field can be be converted to Plugin class
    // by normal GSON without any custom deserializer (CDAP-14515)
    PluginClass pluginClass = new PluginClass("sparkprogram", "wordcount", "desc", "className", "config",
                                              Collections.emptyMap(), Collections.emptySet(), null);
    PluginClass deserializedPluginClass = GSON.fromJson(GSON.toJson(pluginClass), PluginClass.class);
    Assert.assertEquals("wordcount", deserializedPluginClass.getName());
    Assert.assertEquals("sparkprogram", deserializedPluginClass.getType());
    Assert.assertEquals("desc", deserializedPluginClass.getDescription());
    Assert.assertEquals("className", deserializedPluginClass.getClassName());
    Assert.assertTrue(deserializedPluginClass.getRequirements().isEmpty());
  }

  @Test
  public void testValidate() {
    PluginClass invalidPluginClass = new PluginClass();
    String pluginClassJSON = GSON.toJson(invalidPluginClass);
    try {
      PluginClass pluginClass = GSON.fromJson(pluginClassJSON, PluginClass.class);
      pluginClass.validate();
      Assert.fail("Should have failed to convert an invalid json");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
