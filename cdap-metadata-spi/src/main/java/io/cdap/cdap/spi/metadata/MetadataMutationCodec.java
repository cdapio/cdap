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

package io.cdap.cdap.spi.metadata;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * JSON codec for metadata mutations, this is needed to deserialize the correct subclasses of metadata mutataions.
 */
public class MetadataMutationCodec implements JsonSerializer<MetadataMutation>, JsonDeserializer<MetadataMutation> {

  @Override
  public MetadataMutation deserialize(JsonElement json, Type typeOfT,
                                      JsonDeserializationContext context) throws JsonParseException {
    if (!typeOfT.equals(MetadataMutation.class)) {
      return context.deserialize(json, typeOfT);
    }

    String type = json.getAsJsonObject().get("type").getAsString();
    switch (MetadataMutation.Type.valueOf(type)) {
      case CREATE:
        return context.deserialize(json, MetadataMutation.Create.class);
      case REMOVE:
        return context.deserialize(json, MetadataMutation.Remove.class);
      case DROP:
        return context.deserialize(json, MetadataMutation.Drop.class);
      case UPDATE:
        return context.deserialize(json, MetadataMutation.Update.class);
      default:
        throw new IllegalArgumentException(String.format("Unsupported metadata mutation type %s, only " +
                                                           "supported types are CREATE, REMOVE, DROP, UPDATE.", type));
    }
  }

  @Override
  public JsonElement serialize(MetadataMutation src, Type typeOfSrc, JsonSerializationContext context) {
    return context.serialize(src);
  }
}
