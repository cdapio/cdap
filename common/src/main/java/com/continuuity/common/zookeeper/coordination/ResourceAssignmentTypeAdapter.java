/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.common.zookeeper.coordination;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.twill.discovery.Discoverable;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * The Gson codec for {@link ResourceAssignment}.
 */
class ResourceAssignmentTypeAdapter implements JsonSerializer<ResourceAssignment>,
                                               JsonDeserializer<ResourceAssignment> {

  @Override
  public JsonElement serialize(ResourceAssignment src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject json = new JsonObject();
    json.addProperty("name", src.getName());

    src.getAssignments().entries();
    JsonArray assignments = new JsonArray();

    for (Map.Entry<Discoverable, PartitionReplica> entry : src.getAssignments().entries()) {
      JsonArray entryJson = new JsonArray();
      entryJson.add(context.serialize(entry.getKey(), Discoverable.class));
      entryJson.add(context.serialize(entry.getValue()));
      assignments.add(entryJson);
    }

    json.add("assignments", assignments);

    return json;
  }

  @Override
  public ResourceAssignment deserialize(JsonElement json, Type typeOfT,
                                        JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonObject()) {
      throw new JsonParseException("Expect a json object, got " + json);
    }

    JsonObject jsonObj = json.getAsJsonObject();
    String name = jsonObj.get("name").getAsString();

    Multimap<Discoverable, PartitionReplica> assignments = TreeMultimap.create(DiscoverableComparator.COMPARATOR,
                                                                               PartitionReplica.COMPARATOR);
    JsonArray assignmentsJson = context.deserialize(jsonObj.get("assignments"), JsonArray.class);
    for (JsonElement element : assignmentsJson) {
      if (!element.isJsonArray()) {
        throw new JsonParseException("Expect a json array, got " + element);
      }

      JsonArray entryJson = element.getAsJsonArray();
      if (entryJson.size() != 2) {
        throw new JsonParseException("Expect json array of size = 2, got " + entryJson.size());
      }
      Discoverable key = context.deserialize(entryJson.get(0), Discoverable.class);
      PartitionReplica value = context.deserialize(entryJson.get(1), PartitionReplica.class);
      assignments.put(key, value);
    }

    return new ResourceAssignment(name, assignments);
  }
}
