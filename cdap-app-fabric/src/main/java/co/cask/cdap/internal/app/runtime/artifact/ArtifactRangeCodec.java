/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.InvalidArtifactRangeException;
import co.cask.cdap.proto.artifact.ArtifactRanges;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Gson serialize and deserialize {@link ArtifactRange}.
 */
public class ArtifactRangeCodec implements JsonDeserializer<ArtifactRange>, JsonSerializer<ArtifactRange> {

  @Override
  public ArtifactRange deserialize(JsonElement json, Type typeOfT,
                                   JsonDeserializationContext context) throws JsonParseException {
    try {
      return ArtifactRanges.parseArtifactRange(json.getAsString());
    } catch (InvalidArtifactRangeException e) {
      throw new JsonParseException(e);
    }
  }

  @Override
  public JsonElement serialize(ArtifactRange src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(src.toString());
  }
}
