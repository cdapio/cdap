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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.Requirements;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

public class RequirementsCodecTest {

  @Test
  public void testCodec() throws IOException {
    String test1 = getData("Requirements_Pre_6_3.json");
    Gson gsonWithoutCodec = new Gson();
    Requirements requirements = gsonWithoutCodec.fromJson(test1, Requirements.class);
    //Will be null without codec
    Assert.assertNull(requirements.getCapabilities());

    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Requirements.class, new RequirementsCodec())
      .create();
    requirements = gson.fromJson(test1, Requirements.class);
    //not null with codec
    Assert.assertNotNull(requirements.getCapabilities());
    Assert.assertEquals(Arrays.stream(new String[]{"d1", "d2"}).collect(Collectors.toSet()),
                        requirements.getDatasetTypes());

    String test2 = getData("Requirements_6_3.json");
    requirements = gson.fromJson(test2, Requirements.class);
    // Verify serialize and deserialize works as expected
    Assert.assertEquals(Arrays.stream(new String[]{"c1", "c2"}).collect(Collectors.toSet()),
                        requirements.getCapabilities());
    Assert.assertEquals(Arrays.stream(new String[]{"d1", "d2"}).collect(Collectors.toSet()),
                        requirements.getDatasetTypes());
    Assert.assertEquals(requirements, gson.fromJson(gson.toJson(requirements, Requirements.class), Requirements.class));

    String test3 = getData("PluginClass_Pre_6_3.json");
    PluginClass pluginClass = gson.fromJson(test3, PluginClass.class);
    Assert.assertNotNull(pluginClass.getRequirements().getCapabilities());
  }

  private String getData(String fileName) throws IOException {
    URL resource = this.getClass().getResource(String.format("/%s", fileName));
    return Files.readAllLines(Paths.get(resource.getPath())).stream()
      .reduce("", String::concat);
  }

}
