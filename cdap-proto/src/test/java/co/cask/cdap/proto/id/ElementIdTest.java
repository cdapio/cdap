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
import co.cask.cdap.proto.codec.IdTypeAdapter;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class ElementIdTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.class, new IdTypeAdapter())
    .create();

  private final List<? extends ElementId> ids = ImmutableList.<ElementId>builder()
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
    .build();

  @Test
  public void testToFromString() {
    for (ElementId id : ids) {
      doTestToFromString(id);
    }
  }

  private void doTestToFromString(ElementId id) {
    Assert.assertEquals(
      "doTestToFromString failed for class " + id.getClass().getName(),
      id, ElementId.fromString(id.toString(), id.getClass()));
  }

  @Test
  public void testToFromJson() {
    for (ElementId id : ids) {
      doTestToFromJson(id);
    }
  }

  private void doTestToFromJson(ElementId id) {
    Assert.assertEquals(
      "doTestToFromJson failed for class " + id.getClass().getName(),
      id, GSON.fromJson(GSON.toJson(id), id.getClass())
    );
  }

  @Test
  public void testToFromOldId() {
    for (ElementId id : ids) {
      doTestToFromOldId(id);
    }
  }

  private void doTestToFromOldId(ElementId id) {
    Assert.assertEquals(
      "doTestToFromOldId failed for class " + id.getClass().getName(),
      id, id.toId().toElementId());
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

  @Test
  @Ignore
  public void testPrintToString() {
    for (ElementId id : ids) {
      System.out.println(id.toString());
    }
  }

}
