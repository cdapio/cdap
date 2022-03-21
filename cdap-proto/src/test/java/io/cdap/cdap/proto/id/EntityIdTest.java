/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
package io.cdap.cdap.proto.id;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link EntityId}.
 */
public class EntityIdTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() { }.getType();

  private static final List<EntityId> ids = new ArrayList<>();
  static {
    ids.add(Ids.namespace("foo"));
    ids.add(Ids.namespace("foo").artifact("art", "1.2.3"));
    ids.add(Ids.namespace("foo").dataset("zoo"));
    ids.add(Ids.namespace("foo").datasetModule("moo"));
    ids.add(Ids.namespace("foo").datasetType("typ"));
    ids.add(Ids.namespace("foo").app("app"));
    ids.add(Ids.namespace("foo").app("app").workflow("flo"));
    ids.add(Ids.namespace("foo").app("app").mr("flo"));
    ids.add(Ids.namespace("foo").app("app").spark("flo"));
    ids.add(Ids.namespace("foo").app("app").worker("flo"));
    ids.add(Ids.namespace("foo").app("app").service("flo"));
    // components mostly the same
    ids.add(Ids.namespace("zzz"));
    ids.add(Ids.namespace("zzz").artifact("zzz", "1.2.3"));
    ids.add(Ids.namespace("zzz").dataset("zzz"));
    ids.add(Ids.namespace("zzz").datasetModule("zzz"));
    ids.add(Ids.namespace("zzz").datasetType("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").workflow("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").mr("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").spark("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").worker("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").service("zzz"));
  }

  /**
   * To maintain backwards compatibility, don't change this.
   */
  private static final Map<EntityId, String> idsToString = new HashMap<>();
  static {
    idsToString.put(Ids.namespace("foo"), "namespace:foo");
    idsToString.put(Ids.namespace("foo").artifact("art", "1.2.3"), "artifact:foo.art.1.2.3");
    idsToString.put(Ids.namespace("foo").dataset("zoo"), "dataset:foo.zoo");
    idsToString.put(Ids.namespace("foo").datasetModule("moo"), "dataset_module:foo.moo");
    idsToString.put(Ids.namespace("foo").datasetType("typ"), "dataset_type:foo.typ");
    idsToString.put(Ids.namespace("foo").app("app"), "application:foo.app.-SNAPSHOT");
    idsToString.put(Ids.namespace("foo").app("app", "v1"), "application:foo.app.v1");
  }

  /**
   * To maintain backwards compatibility, don't change this.
   */
  private static final Map<EntityId, String> idsToJson = new HashMap<>();
  static {
    idsToJson.put(Ids.namespace("foo"), "{\"namespace\":\"foo\",\"entity\":\"NAMESPACE\"}");
    idsToJson.put(
      Ids.namespace("foo").artifact("art", "1.2.3"),
      "{\"namespace\":\"foo\",\"artifact\":\"art\",\"version\":\"1.2.3\",\"entity\":\"ARTIFACT\"}");
    idsToJson.put(
      Ids.namespace("foo").dataset("zoo"),
      "{\"namespace\":\"foo\",\"dataset\":\"zoo\",\"entity\":\"DATASET\"}");
    idsToJson.put(
      Ids.namespace("foo").datasetModule("moo"),
      "{\"namespace\":\"foo\",\"module\":\"moo\",\"entity\":\"DATASET_MODULE\"}");
    idsToJson.put(
      Ids.namespace("foo").datasetType("typ"),
      "{\"namespace\":\"foo\",\"type\":\"typ\",\"entity\":\"DATASET_TYPE\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"version\":\"-SNAPSHOT\",\"entity\":\"APPLICATION\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app", "v1"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"version\":\"v1\",\"entity\":\"APPLICATION\"}");
  }

  @Test
  public void testToFromString() {
    for (EntityId id : ids) {
      doTestToFromString(id);
    }
  }

  private void doTestToFromString(EntityId id) {
    Assert.assertEquals(
      "doTestToFromString failed for class " + id.getClass().getName(),
      id, EntityId.fromString(id.toString(), id.getClass()));
  }

  @Test
  public void testToFromJson() {
    for (EntityId id : ids) {
      doTestToFromJson(id);
    }
  }

  private void doTestToFromJson(EntityId id) {
    Assert.assertEquals(
      "doTestToFromJson failed for class " + id.getClass().getName(),
      id, GSON.fromJson(GSON.toJson(id), id.getClass())
    );
    Assert.assertEquals(
      "doTestToFromJson failed for class " + id.getClass().getName(),
      id, GSON.fromJson(GSON.toJson(id), EntityId.class)
    );
  }

  @Test
  public void testDatasetName() {
    DatasetId.fromString("dataset:foo.Zo123_-$$_-");
    DatasetId dataset = DatasetId.fromString("dataset:foo.app.meta");
    Assert.assertEquals("foo", dataset.getNamespace());
    Assert.assertEquals("app.meta", dataset.getDataset());
    dataset = DatasetId.fromString("dataset:foo.app.meta.second");
    Assert.assertEquals("app.meta.second", dataset.getDataset());
    try {
      DatasetId.fromString("dataset:i have a space in my name");
      Assert.fail("Dataset Id with space in its name was created successfully while it should have failed.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      DatasetId.fromString("dataset:.should.have.a.namespace.name");
      Assert.fail("Dataset Id without namespace in its name was created successfully while it should have failed.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    // test error message
    try {
      // missing dataset field
      DatasetId.fromString("dataset:foo");
      Assert.fail("Dataset Id creation should fail without dataset name");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Missing field: dataset", e.getCause().getMessage());
    }
  }

  @Test
  public void testDatasetIdGetParent() {
    DatasetId datasetId = new NamespaceId("foo").dataset("testParent");
    String datasetIdJson = GSON.toJson(datasetId);
    Assert.assertEquals(
      datasetId.getNamespace(), GSON.fromJson(datasetIdJson, DatasetId.class).getParent().getNamespace()
    );
  }

  @Test
  public void testMissingApplicationName() {
    try {
      ApplicationId.fromString("application:ns1");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
      Assert.assertEquals("Missing field: application", e.getCause().getMessage());
    }
  }

  @Test
  public void testAppNameWithDot() {
    try {
      new ApplicationId("ns", "app.name");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
      Assert.assertTrue(e.getMessage().contains("Invalid application ID"));
    }
  }

  /**
   * This shouldn't be changed, so that we can maintain backwards compatibility.
   */
  @Test
  public void testCompatibility() {
    for (Map.Entry<? extends EntityId, String> toStringEntry : idsToString.entrySet()) {
      Assert.assertEquals(toStringEntry.getValue(), toStringEntry.getKey().toString());
    }

    for (Map.Entry<? extends EntityId, String> toJsonEntry : idsToJson.entrySet()) {
      Assert.assertEquals(jsonToMap(toJsonEntry.getValue()), jsonToMap(GSON.toJson(toJsonEntry.getKey())));
    }
  }

  @Test
  public void testFromMetadataEntity() {
    ApplicationId applicationId = new ApplicationId("testNs", "app1");
    Assert.assertEquals(applicationId, EntityId.fromMetadataEntity(applicationId.toMetadataEntity()));

    MetadataEntity metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "testNs")
      .appendAsType(MetadataEntity.APPLICATION, "app1").build();
    Assert.assertEquals(applicationId, EntityId.fromMetadataEntity(metadataEntity));

    metadataEntity = MetadataEntity.builder(MetadataEntity.ofDataset("testNs", "testDs"))
      .appendAsType("field", "testField").build();
    try {
      EntityId.fromMetadataEntity(metadataEntity);
      Assert.fail("Should have failed to get create an EntityId from MetadataEntity");
    } catch (IllegalArgumentException e) {
      // expected
    }

    ProgramId programId = new ProgramId(applicationId, ProgramType.WORKER, "testWorker");
    Assert.assertEquals(programId, EntityId.fromMetadataEntity(programId.toMetadataEntity()));

    // artifact
    ArtifactId artifactId = new ArtifactId("testNs", "testArtifact-1.0.0.jar");
    Assert.assertEquals(artifactId, EntityId.fromMetadataEntity(artifactId.toMetadataEntity()));

    // schedule
    ScheduleId scheduleId = applicationId.schedule("sch");
    metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "testNs")
      .append(MetadataEntity.APPLICATION, "app1").appendAsType(MetadataEntity.SCHEDULE, "sch").build();
    Assert.assertEquals(scheduleId, EntityId.fromMetadataEntity(metadataEntity));
  }

  @Test
  public void testGetSelfOrParentEntityId() {
    // entity is a known entityId
    MetadataEntity metadataEntity = MetadataEntity.ofDataset("testNs", "testDs");
    Assert.assertEquals(new DatasetId("testNs", "testDs"), EntityId.getSelfOrParentEntityId(metadataEntity));

    // entity's parent is not a known entity
    metadataEntity = MetadataEntity.builder().append("ab", "cd").appendAsType("unkonwType", "value")
      .append("ef", "gh").build();
    try {
      EntityId.getSelfOrParentEntityId(metadataEntity);
      Assert.fail("Should have failed to get a parent entity id");
    } catch (IllegalArgumentException e) {
      // expected
    }

    metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "testNs")
      .append(MetadataEntity.DATASET, "testDs").appendAsType("field", "testField").build();
    Assert.assertEquals(new DatasetId("testNs", "testDs"), EntityId.getSelfOrParentEntityId(metadataEntity));

    metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "testNs")
      .append(MetadataEntity.APPLICATION, "testApp")
      .appendAsType("custom", "custValue").build();
    Assert.assertEquals(new ApplicationId("testNs", "testApp"), EntityId.getSelfOrParentEntityId(metadataEntity));

    metadataEntity = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "testNs")
      .append(MetadataEntity.APPLICATION, "testApp")
      .append(MetadataEntity.TYPE, ProgramType.WORKER.getPrettyName()).append(MetadataEntity.PROGRAM, "testProg")
      .appendAsType("subType", "realtime").build();
    Assert.assertEquals(new ProgramId("testNs", "testApp", ProgramType.WORKER, "testProg"),
                        EntityId.getSelfOrParentEntityId(metadataEntity));

    ArtifactId artifactId = new ArtifactId("testNs", "testArtifact-1.0.0.jar");
    metadataEntity = MetadataEntity.builder(artifactId.toMetadataEntity()).appendAsType("time", "t2").build();
    Assert.assertEquals(artifactId, EntityId.getSelfOrParentEntityId(metadataEntity));
  }

  @Test
  public void testHierarchy() {
    NamespaceId namespace = Ids.namespace("foo");
    ApplicationId app = namespace.app("bar");
    ProgramId program = app.service("foo");

    List<EntityId> expectedHierarchy = new ArrayList<>();
    expectedHierarchy.add(namespace);
    Assert.assertEquals(expectedHierarchy, Lists.newArrayList(namespace.getHierarchy()));
    Assert.assertEquals(expectedHierarchy, Lists.newArrayList(namespace.getHierarchy(true)));
    expectedHierarchy.add(app);
    Assert.assertEquals(expectedHierarchy, Lists.newArrayList(app.getHierarchy()));
    Assert.assertEquals(Lists.reverse(expectedHierarchy), Lists.newArrayList(app.getHierarchy(true)));
    expectedHierarchy.add(program);
    Assert.assertEquals(expectedHierarchy, Lists.newArrayList(program.getHierarchy()));
    Assert.assertEquals(Lists.reverse(expectedHierarchy), Lists.newArrayList(program.getHierarchy(true)));
  }

/*  @Test(expected = UnsupportedOperationException.class)
  public void testInstanceId() {
    InstanceId instanceId = new InstanceId("mycdap");
    instanceId.toId();
  }*/

  @Test
  public void testArtifactId() {
    Assert.assertTrue(ArtifactId.isValidArtifactId("cdap-artifact-id_2.10-1.0.0"));
    Assert.assertTrue(ArtifactId.isValidArtifactId("cdap-artifact-id-1.0.0-2.10"));
  }

  @Test
  @Ignore
  public void printToString() {
    for (EntityId id : ids) {
      System.out.println(id.getEntityType() + ": " + id.toString());
    }
  }

  @Test
  @Ignore
  public void printToJson() {
    for (EntityId id : ids) {
      System.out.println(id.getEntityType() + ": " + GSON.toJson(id));
    }
  }

  private Map<String, Object> jsonToMap(String json) {
    return GSON.fromJson(json, MAP_TYPE);
  }
}
