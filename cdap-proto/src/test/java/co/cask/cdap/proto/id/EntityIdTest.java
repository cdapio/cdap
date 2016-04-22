/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.codec.IdTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link EntityId}.
 */
public class EntityIdTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.class, new IdTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private static final List<EntityId> ids = new ArrayList<>();
  static {
    ids.add(Ids.namespace("foo"));
    ids.add(Ids.namespace("foo").artifact("art", "1.2.3"));
    ids.add(Ids.namespace("foo").dataset("zoo"));
    ids.add(Ids.namespace("foo").datasetModule("moo"));
    ids.add(Ids.namespace("foo").datasetType("typ"));
    ids.add(Ids.namespace("foo").stream("t"));
    ids.add(Ids.namespace("foo").app("app"));
    ids.add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"));
    ids.add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"));
    ids.add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"));
    ids.add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"));
    ids.add(Ids.namespace("foo").app("app").flow("flo"));
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
    ids.add(Ids.namespace("zzz").stream("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").run("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").flowlet("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").flowlet("zzz").queue("zzz"));
    ids.add(Ids.namespace("zzz").app("zzz").flow("zzz"));
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
    idsToString.put(Ids.namespace("foo").stream("sdf"), "stream:foo.sdf");
    idsToString.put(Ids.namespace("foo").app("app"), "application:foo.app");
    idsToString.put(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"), "program:foo.app.flow.flo");
    idsToString.put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"),
        "program_run:foo.app.flow.flo.run1");
    idsToString.put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"),
        "flowlet:foo.app.flo.flol");
    idsToString.put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"),
        "flowlet_queue:foo.app.flo.flol.q");
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
      Ids.namespace("foo").stream("t"),
      "{\"namespace\":\"foo\",\"stream\":\"t\",\"entity\":\"STREAM\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"entity\":\"APPLICATION\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"type\":\"Flow\",\"program\":\"flo\",\"entity\":\"PROGRAM\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"type\":\"Flow\",\"program\":\"flo\"," +
        "\"run\":\"run1\",\"entity\":\"PROGRAM_RUN\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"flow\":\"flo\",\"flowlet\":\"flol\",\"entity\":\"FLOWLET\"}");
    idsToJson.put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"),
      "{\"namespace\":\"foo\",\"application\":\"app\",\"flow\":\"flo\"," +
        "\"flowlet\":\"flol\",\"queue\":\"q\",\"entity\":\"FLOWLET_QUEUE\"}");
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
  public void testToFromOldId() {
    for (EntityId id : ids) {
      doTestToFromOldId(id);
    }
  }

  private void doTestToFromOldId(EntityId id) {
    Assert.assertEquals(
      "doTestToFromOldId failed for class " + id.getClass().getName(),
      id, id.toId().toEntityId());
  }

  @Test
  public void testDatasetName() {
    // TODO: CDAP-5730: Even though dots are allowed, if we use it here, id creation will fail.
    DatasetId.fromString("dataset:foo.Zo123_-$$_-");
    try {
      DatasetId.fromString("dataset:i have a space in my name");
      Assert.fail("Dataset Id with space in its name was created successfully while it should have failed.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testInvalidId() {
    try {
      ApplicationId.fromString("application:ns1");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
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
      Assert.assertEquals(toJsonEntry.getValue(), GSON.toJson(toJsonEntry.getKey()));
    }
  }

  @Test
  public void testHierarchy() {
    NamespaceId namespace = Ids.namespace("foo");
    ApplicationId app = namespace.app("bar");
    ProgramId program = app.flow("foo");

    List<EntityId> expectedHierarchy = new ArrayList<>();
    expectedHierarchy.add(namespace);
    Assert.assertEquals(expectedHierarchy, namespace.getHierarchy());
    expectedHierarchy.add(app);
    Assert.assertEquals(expectedHierarchy, app.getHierarchy());
    expectedHierarchy.add(program);
    Assert.assertEquals(expectedHierarchy, program.getHierarchy());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInstanceId() {
    InstanceId instanceId = new InstanceId("mycdap");
    instanceId.toId();
  }

  @Test
  @Ignore
  public void printToString() {
    for (EntityId id : ids) {
      System.out.println(id.getEntity() + ": " + id.toString());
    }
  }

  @Test
  @Ignore
  public void printToJson() {
    for (EntityId id : ids) {
      System.out.println(id.getEntity() + ": " + GSON.toJson(id));
    }
  }

}
