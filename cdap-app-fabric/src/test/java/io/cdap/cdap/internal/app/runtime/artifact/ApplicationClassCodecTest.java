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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.Requirements;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

public class ApplicationClassCodecTest {

  @Test
  public void testCodec() throws IOException {
    Gson gson = new GsonBuilder()
      .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
      .registerTypeAdapter(ApplicationClass.class, new ApplicationClassCodec())
      .create();
    String testApp1 = getData("ApplicationClass_6_2.json");
    ApplicationClass applicationClass62 = gson.fromJson(testApp1, ApplicationClass.class);

    Assert.assertEquals(Requirements.EMPTY, applicationClass62.getRequirements());
    Assert.assertEquals("io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp",
                        applicationClass62.getClassName());
    Assert.assertEquals("", applicationClass62.getDescription());
    Assert.assertNotNull(applicationClass62.getConfigSchema());

    Assert.assertEquals(applicationClass62, gson.fromJson(gson.toJson(applicationClass62), ApplicationClass.class));

    String testApp2 = getData("ApplicationClass_6_3.json");
    ApplicationClass applicationClass63 = gson.fromJson(testApp2, ApplicationClass.class);
    Requirements requirements = applicationClass63.getRequirements();
    Assert.assertEquals(Collections.singleton("cdc"), requirements.getCapabilities());

    Assert.assertEquals(applicationClass63, gson.fromJson(gson.toJson(applicationClass63), ApplicationClass.class));
  }

  private String getData(String fileName) throws IOException {
    URL resource = this.getClass().getResource(String.format("/%s", fileName));
    return Files.readAllLines(Paths.get(resource.getPath())).stream()
      .reduce("", String::concat);
  }
}
