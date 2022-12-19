/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.id.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for testing {@link MetadataEntityCodec}
 */
public class MetadataEntityCodecTest {

  @Test
  public void testVersionlessEntitySerialization() {

    Gson gson = new GsonBuilder().registerTypeAdapter(MetadataEntity.class, new MetadataEntityCodec())
      .create();

    // Test serialization/deserialization for application MetadataEntity
    MetadataEntity appMetadata = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
      .build();
    String json = gson.toJson(appMetadata);
    // make sure version is removed from Application MetadataEntity json
    Assert.assertFalse(json.contains(MetadataEntity.VERSION));
    // deserialization should add back the default version
    Assert.assertEquals(appMetadata, gson.fromJson(json, MetadataEntity.class));

    // Test serialization/deserialization for program MetadataEntity
    MetadataEntity programMetadata = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
      .append(MetadataEntity.TYPE, "Service")
      .appendAsType(MetadataEntity.PROGRAM, "myProgram")
      .build();
    json = gson.toJson(programMetadata);
    Assert.assertFalse(json.contains(MetadataEntity.VERSION));
    Assert.assertEquals(programMetadata, gson.fromJson(json, MetadataEntity.class));

    // Test serialization/deserialization for schedule MetadataEntity
    MetadataEntity scheduleMetadata = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.SCHEDULE, "mySchedule")
      .build();
    json = gson.toJson(scheduleMetadata);
    Assert.assertFalse(json.contains(MetadataEntity.VERSION));
    Assert.assertEquals(scheduleMetadata, gson.fromJson(json, MetadataEntity.class));

    // Test serialization/deserialization for program_run MetadataEntity
    MetadataEntity programRunMetadata =  MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.APPLICATION, "myApp")
      .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
      .append(MetadataEntity.TYPE, "Service")
      .append(MetadataEntity.PROGRAM, "myProgram")
      .appendAsType(MetadataEntity.PROGRAM_RUN, "myProgramRun")
      .build();
    json = gson.toJson(programRunMetadata);
    Assert.assertFalse(json.contains(MetadataEntity.VERSION));
    Assert.assertEquals(programRunMetadata, gson.fromJson(json, MetadataEntity.class));
  }

  @Test
  public void testVersionedMetadataEntitySerialization() {

    Gson gson = new GsonBuilder().registerTypeAdapter(MetadataEntity.class, new MetadataEntityCodec())
      .setPrettyPrinting().create();

    // test serialize artifact
    MetadataEntity artifactMetadata = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .appendAsType(MetadataEntity.ARTIFACT, "myArtifact")
      .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
      .build();
    String json = gson.toJson(artifactMetadata);
    Assert.assertTrue(json.contains(MetadataEntity.VERSION));
    Assert.assertEquals(artifactMetadata, gson.fromJson(json, MetadataEntity.class));

    // test serialize plugin
    MetadataEntity pluginMetadata = MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, "ns")
      .append(MetadataEntity.ARTIFACT, "myArtifact")
      .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
      .append(MetadataEntity.TYPE, "myType")
      .appendAsType(MetadataEntity.PLUGIN, "myPlugin")
      .build();
    json = gson.toJson(pluginMetadata);
    Assert.assertTrue(json.contains(MetadataEntity.VERSION));
    Assert.assertEquals(pluginMetadata, gson.fromJson(json, MetadataEntity.class));
  }
}
