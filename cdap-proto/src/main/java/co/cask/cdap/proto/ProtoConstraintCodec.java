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

package co.cask.cdap.proto;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Serialization and deserialization of run constraints as Json.
 */
public class ProtoConstraintCodec implements JsonSerializer<ProtoConstraint>, JsonDeserializer<ProtoConstraint> {

  /**
   * Maps each type to a class for deserialization.
   */
  private static final Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> TYPE_TO_CONSTRAINT = generateMap();

  private static Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> generateMap() {
    Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> map = new HashMap<>();
    map.put(ProtoConstraint.Type.CONCURRENCY, ProtoConstraint.ConcurrenyConstraint.class);
    map.put(ProtoConstraint.Type.DELAY, ProtoConstraint.DelayConstraint.class);
    map.put(ProtoConstraint.Type.LAST_RUN, ProtoConstraint.LastRunConstraint.class);
    map.put(ProtoConstraint.Type.TIME_RANGE, ProtoConstraint.TimeRangeConstraint.class);
    return map;
  }

  /**
   * Maps each run constraint type to a class for deserialization.
   */
  private final Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> typeClassMap;

  /**
   * Constructs a Codec with the default mapping from run constraint type to run constraint class.
   */
  public ProtoConstraintCodec() {
    this(TYPE_TO_CONSTRAINT);
  }

  /**
   * Constructs a Codec with a custom mapping from run constraint type to run constraint class.
   */
  protected ProtoConstraintCodec(Map<ProtoConstraint.Type, Class<? extends ProtoConstraint>> typeClassMap) {
    this.typeClassMap = typeClassMap;
  }

  @Override
  public JsonElement serialize(ProtoConstraint src, Type typeOfSrc, JsonSerializationContext context) {
    // this assumes that ProtoConstraint is an abstract class (every instance will have a concrete subclass type)
    return context.serialize(src, src.getClass());
  }

  @Override
  public ProtoConstraint deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (json == null) {
      return null;
    }
    if (!(json instanceof JsonObject)) {
      throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
    }
    JsonObject object = (JsonObject) json;
    JsonElement typeJson = object.get("type");
    ProtoConstraint.Type constraintType = context.deserialize(typeJson, ProtoConstraint.Type.class);
    Class<? extends ProtoConstraint> subClass = typeClassMap.get(constraintType);
    if (subClass == null) {
      throw new JsonParseException("Unable to map constraint type " + constraintType + " to a run constraint class");
    }
    ProtoConstraint constraint = context.deserialize(json, subClass);
    constraint.validate();
    return constraint;
  }
}
