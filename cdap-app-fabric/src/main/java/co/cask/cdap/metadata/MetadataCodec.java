/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.ScopedName;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JSON Type adapter for Metadata objects. This is needed because the properties of the metadata
 * are a map from ScopedName (scope + name) to value. Since Json only allows string values as
 * map keys, we serialize the map as a list of its entries.
 */
public class MetadataCodec implements JsonSerializer<Metadata>, JsonDeserializer<Metadata> {

  private static final Type SET_SCOPED_NAME_TYPE = new TypeToken<Set<ScopedName>>() { }.getType();
  private static final Type LIST_PROPERTY_TYPE = new TypeToken<List<Property>>() { }.getType();

  @Override
  public Metadata deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    if (!typeOfT.equals(Metadata.class)) {
      return context.deserialize(json, typeOfT);
    }
    JsonObject object = json.getAsJsonObject();
    JsonElement tagsJson = object.get("tags");
    JsonElement propsJson = object.get("properties");
    Set<ScopedName> tags = context.deserialize(tagsJson, SET_SCOPED_NAME_TYPE);
    List<Property> props = context.deserialize(propsJson, LIST_PROPERTY_TYPE);
    Map<ScopedName, String> properties = props.stream().collect(Collectors.toMap(
      x -> new ScopedName(x.getScope(), x.getName()), Property::getValue));
    return new Metadata(tags, properties);
  }

  @Override
  public JsonElement serialize(Metadata src, Type typeOfSrc, JsonSerializationContext context) {
    JsonElement tags = context.serialize(src.getTags());
    JsonElement props = context.serialize(src.getProperties().entrySet().stream().map(
      entry -> new Property(entry.getKey().getScope(), entry.getKey().getName(), entry.getValue())
    ).collect(Collectors.toList()));
    JsonObject object = new JsonObject();
    object.add("tags", tags);
    object.add("properties", props);
    return object;
  }

  static class Property {
    private final MetadataScope scope;
    private final String name;
    private final String value;

    Property(MetadataScope scope, String name, String value) {
      this.scope = scope;
      this.name = name;
      this.value = value;
    }

    public MetadataScope getScope() {
      return scope;
    }

    public String getName() {
      return name;
    }

    public String getValue() {
      return value;
    }
  }
}
