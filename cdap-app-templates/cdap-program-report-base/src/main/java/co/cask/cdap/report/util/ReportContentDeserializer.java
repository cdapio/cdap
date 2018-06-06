/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.util;

import co.cask.cdap.report.proto.ReportContent;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Deserializer of {@link ReportContent} from Json.
 */
public class ReportContentDeserializer  implements JsonDeserializer<ReportContent> {

  @Override
  public ReportContent deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
      if (json == null) {
        return null;
      }
      if (!(json instanceof JsonObject)) {
        throw new JsonParseException("Expected a JsonObject but found a " + json.getClass().getName());
      }
    JsonObject object = (JsonObject) json;
    JsonArray detailsJson = object.getAsJsonArray("details");
    List<String> details = detailsJson == null ? null : new ArrayList<>();
    if (detailsJson != null) {
      Iterator<JsonElement> iter = detailsJson.iterator();
      while (iter.hasNext()) {
        details.add(iter.next().toString());
      }
    }
    return new ReportContent(object.getAsJsonPrimitive("offset").getAsLong(),
                             object.getAsJsonPrimitive("limit").getAsInt(),
                             object.getAsJsonPrimitive("total").getAsInt(),
                             details);
  }
}
