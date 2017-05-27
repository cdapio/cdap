/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule.constraint;

import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.ProtoConstraint;
import co.cask.cdap.proto.ProtoConstraintCodec;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.TimeZone;

public class ConstraintCodecTest {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Constraint.class, new ConstraintCodec())
    .create();

  private static final Gson GSON_PROTO = new GsonBuilder()
    .registerTypeAdapter(Constraint.class, new ProtoConstraintCodec())
    .create();

  @Test
  public void testConstraintCodec() {
    testSerDeser(new ProtoConstraint.ConcurrencyConstraint(3), new ConcurrencyConstraint(3));
    testSerDeser(new ProtoConstraint.DelayConstraint(300000L), new DelayConstraint(300000L));
    testSerDeser(new ProtoConstraint.LastRunConstraint(3600000L), new LastRunConstraint(3600000L));
    testSerDeser(new ProtoConstraint.TimeRangeConstraint("02:00", "06:00", TimeZone.getDefault()),
                 new TimeRangeConstraint("02:00", "06:00", TimeZone.getDefault()));
  }

  private void testSerDeser(ProtoConstraint proto, Constraint constraint) {
    String jsonOfConstraint = GSON.toJson(constraint);
    String jsonOfConstraintAsConstraint = GSON.toJson(constraint, Constraint.class);
    String jsonOfProto = GSON.toJson(proto);
    String jsonOfProtoAsConstraint = GSON.toJson(proto, Constraint.class);
    String jsonOfConstraintByProto = GSON_PROTO.toJson(constraint);
    String jsonOfConstraintAsConstraintByProto = GSON_PROTO.toJson(constraint, Constraint.class);
    String jsonOfProtoByProto = GSON_PROTO.toJson(proto);
    String jsonOfProtoAsConstraintByProto = GSON_PROTO.toJson(proto, Constraint.class);

    Assert.assertEquals(jsonOfConstraint, jsonOfConstraintAsConstraint);
    Assert.assertEquals(jsonOfConstraint, jsonOfProto);
    Assert.assertEquals(jsonOfConstraint, jsonOfProtoAsConstraint);
    Assert.assertEquals(jsonOfConstraint, jsonOfConstraintByProto);
    Assert.assertEquals(jsonOfConstraint, jsonOfConstraintAsConstraintByProto);
    Assert.assertEquals(jsonOfConstraint, jsonOfProtoByProto);
    Assert.assertEquals(jsonOfConstraint, jsonOfProtoAsConstraintByProto);

    Constraint deserialized = GSON.fromJson(jsonOfConstraint, Constraint.class);
    Constraint deserializedAsProto = GSON_PROTO.fromJson(jsonOfConstraint, Constraint.class);

    Assert.assertEquals(constraint, deserialized);
    Assert.assertEquals(proto, deserializedAsProto);
  }
}
