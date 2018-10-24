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

public class PluginClassTest {
  private static final Gson GSON = new Gson();

  @Test
  public void test() {
    String oldPluginClass = "{ \"type\": \"sparkprogram\", \"name\": \"wordcount\", \"description\": \"desc\", " +
      "\"className\": \"co.cask.cdap.datapipeline.spark.WordCount\", \"properties\": {}, \"endpoints\": [] }";
    PluginClass pluginClass = GSON.fromJson(oldPluginClass, PluginClass.class);
    Assert.assertEquals("wordcount", pluginClass.getName());
    Assert.assertEquals("sparkprogram", pluginClass.getType());
    Assert.assertEquals("desc", pluginClass.getDescription());
    Assert.assertEquals("co.cask.cdap.datapipeline.spark.WordCount", pluginClass.getClassName());
    Assert.assertTrue(pluginClass.getRequirements().isEmpty());

    String namelessPluginClass = "{ \"type\": \"sparkprogram\", \"description\": \"\", \"className\": \"co.cask.cdap" +
      ".datapipeline.spark.WordCount\", \"properties\": {}, \"endpoints\": [] }";
    pluginClass = GSON.fromJson(namelessPluginClass, PluginClass.class);
    Assert.assertEquals("wordcount", pluginClass.getName());
    Assert.assertEquals("sparkprogram", pluginClass.getType());
    Assert.assertTrue(pluginClass.getRequirements().isEmpty());
  }
}
