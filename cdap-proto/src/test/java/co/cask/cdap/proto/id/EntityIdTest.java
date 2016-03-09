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
package co.cask.cdap.proto.id;

import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.codec.IdTypeAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 *
 */
public class EntityIdTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.class, new IdTypeAdapter())
    .registerTypeAdapter(EntityId.class, new EntityIdTypeAdapter())
    .create();

  private final List<? extends EntityId> ids = ImmutableList.<EntityId>builder()
    .add(Ids.namespace("foo"))
    .add(Ids.namespace("foo").artifact("art", "1.2.3"))
    .add(Ids.namespace("foo").dataset("zoo"))
    .add(Ids.namespace("foo").datasetModule("moo"))
    .add(Ids.namespace("foo").datasetType("typ"))
    .add(Ids.namespace("foo").stream("t"))
    .add(Ids.namespace("foo").app("app"))
    .add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"))
    .add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"))
    .add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"))
    .add(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"))
    .add(Ids.namespace("foo").app("app").flow("flo"))
    .add(Ids.namespace("foo").app("app").workflow("flo"))
    .add(Ids.namespace("foo").app("app").mr("flo"))
    .add(Ids.namespace("foo").app("app").spark("flo"))
    .add(Ids.namespace("foo").app("app").worker("flo"))
    .add(Ids.namespace("foo").app("app").service("flo"))
    // components mostly the same
    .add(Ids.namespace("zzz"))
    .add(Ids.namespace("zzz").artifact("zzz", "1.2.3"))
    .add(Ids.namespace("zzz").dataset("zzz"))
    .add(Ids.namespace("zzz").datasetModule("zzz"))
    .add(Ids.namespace("zzz").datasetType("zzz"))
    .add(Ids.namespace("zzz").stream("zzz"))
    .add(Ids.namespace("zzz").app("zzz"))
    .add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz"))
    .add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").run("zzz"))
    .add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").flowlet("zzz"))
    .add(Ids.namespace("zzz").app("zzz").program(ProgramType.FLOW, "zzz").flowlet("zzz").queue("zzz"))
    .add(Ids.namespace("zzz").app("zzz").flow("zzz"))
    .add(Ids.namespace("zzz").app("zzz").workflow("zzz"))
    .add(Ids.namespace("zzz").app("zzz").mr("zzz"))
    .add(Ids.namespace("zzz").app("zzz").spark("zzz"))
    .add(Ids.namespace("zzz").app("zzz").worker("zzz"))
    .add(Ids.namespace("zzz").app("zzz").service("zzz"))
    .build();

  /**
   * To maintain backwards compatibility, don't change this.
   */
  private final Map<? extends EntityId, String> idsToString = ImmutableMap.<EntityId, String>builder()
    .put(Ids.namespace("foo"), "namespace:foo")
    .put(Ids.namespace("foo").artifact("art", "1.2.3"), "artifact:foo.art.1.2.3")
    .put(Ids.namespace("foo").dataset("zoo"), "dataset:foo.zoo")
    .put(Ids.namespace("foo").datasetModule("moo"), "dataset_module:foo.moo")
    .put(Ids.namespace("foo").datasetType("typ"), "dataset_type:foo.typ")
    .put(Ids.namespace("foo").stream("sdf"), "stream:foo.sdf")
    .put(Ids.namespace("foo").app("app"), "application:foo.app")
    .put(Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"), "program:foo.app.flow.flo")
    .put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"),
      "program_run:foo.app.flow.flo.run1")
    .put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"),
      "flowlet:foo.app.flo.flol")
    .put(
      Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"),
      "flowlet_queue:foo.app.flo.flol.q")
    .build();

  /**
   * To maintain backwards compatibility, don't change this.
   */
  private final Map<? extends EntityId, String> idsToJson =
    ImmutableMap.<EntityId, String>builder()
      .put(Ids.namespace("foo"), "{\"namespace\":\"foo\",\"entity\":\"NAMESPACE\"}")
      .put(
        Ids.namespace("foo").artifact("art", "1.2.3"),
        "{\"namespace\":\"foo\",\"artifact\":\"art\",\"version\":\"1.2.3\",\"entity\":\"ARTIFACT\"}")
      .put(
        Ids.namespace("foo").dataset("zoo"),
        "{\"namespace\":\"foo\",\"dataset\":\"zoo\",\"entity\":\"DATASET\"}")
      .put(
        Ids.namespace("foo").datasetModule("moo"),
        "{\"namespace\":\"foo\",\"module\":\"moo\",\"entity\":\"DATASET_MODULE\"}")
      .put(
        Ids.namespace("foo").datasetType("typ"),
        "{\"namespace\":\"foo\",\"type\":\"typ\",\"entity\":\"DATASET_TYPE\"}")
      .put(
        Ids.namespace("foo").stream("t"),
        "{\"namespace\":\"foo\",\"stream\":\"t\",\"entity\":\"STREAM\"}")
      .put(
        Ids.namespace("foo").app("app"),
        "{\"namespace\":\"foo\",\"application\":\"app\",\"entity\":\"APPLICATION\"}")
      .put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo"),
        "{\"namespace\":\"foo\",\"application\":\"app\",\"type\":\"Flow\",\"program\":\"flo\",\"entity\":\"PROGRAM\"}")
      .put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").run("run1"),
        "{\"namespace\":\"foo\",\"application\":\"app\",\"type\":\"Flow\",\"program\":\"flo\"," +
          "\"run\":\"run1\",\"entity\":\"PROGRAM_RUN\"}")
      .put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol"),
        "{\"namespace\":\"foo\",\"application\":\"app\",\"flow\":\"flo\",\"flowlet\":\"flol\",\"entity\":\"FLOWLET\"}")
      .put(
        Ids.namespace("foo").app("app").program(ProgramType.FLOW, "flo").flowlet("flol").queue("q"),
        "{\"namespace\":\"foo\",\"application\":\"app\",\"flow\":\"flo\"," +
          "\"flowlet\":\"flol\",\"queue\":\"q\",\"entity\":\"FLOWLET_QUEUE\"}")
      .build();

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

    Assert.assertEquals(ImmutableList.of(namespace), ImmutableList.copyOf(namespace.getHierarchy()));
    Assert.assertEquals(ImmutableList.of(namespace, app), ImmutableList.copyOf(app.getHierarchy()));
    Assert.assertEquals(ImmutableList.of(namespace, app, program), ImmutableList.copyOf(program.getHierarchy()));
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
