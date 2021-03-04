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

package io.cdap.cdap.proto.codec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link EntityIdTypeAdapter}.
 */
public class EntityIdTypeAdapterTest {
  private static Gson gson;

  @BeforeClass
  public static void init() {
    gson = new GsonBuilder()
      .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
      .create();
  }

  @Test
  public void testSerializeEntityIdWithNoParent() {
    NamespaceId namespaceEntity = new NamespaceId("test_namespace");
    String serialized = gson.toJson(namespaceEntity);
    EntityId deserializedEntity = gson.fromJson(serialized, EntityId.class);
    Assert.assertEquals(namespaceEntity, deserializedEntity);
  }

  @Test
  public void testSerializeEntityIdWithParent() {
    ArtifactId artifactEntity = new ArtifactId("test_namespace", "test_artifact", "1.0");
    gson.toJson(artifactEntity);
    String serialized = gson.toJson(artifactEntity);
    EntityId deserializedEntity = gson.fromJson(serialized, EntityId.class);
    Assert.assertEquals(artifactEntity, deserializedEntity);
  }

  @Test
  public void testSerializeEntityIdWithParentWithHierarchy() {
    ArtifactId artifactEntity = new ArtifactId("test_namespace", "test_artifact", "1.0");
    artifactEntity.getHierarchy();
    String serialized = gson.toJson(artifactEntity);
    EntityId deserializedEntity = gson.fromJson(serialized, EntityId.class);
    Assert.assertEquals(artifactEntity, deserializedEntity);
  }
}
