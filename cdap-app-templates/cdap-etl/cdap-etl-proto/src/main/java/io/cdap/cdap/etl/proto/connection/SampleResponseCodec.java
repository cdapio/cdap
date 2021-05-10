/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.etl.proto.connection;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Codec to serialize/deserialize sample response
 */
public class SampleResponseCodec implements JsonSerializer<SampleResponse>, JsonDeserializer<SampleResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(SampleResponseCodec.class);

  @Override
  public SampleResponse deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    ConnectorSpec spec = context.deserialize(jsonObj.get("spec"), ConnectorSpec.class);
    JsonElement schemaJson = jsonObj.get("schema");
    Schema schema = schemaJson == null ? null : context.deserialize(schemaJson, Schema.class);
    if (schema == null) {
      return new SampleResponse(spec, schema, Collections.emptyList());
    }

    JsonArray jsonArray = jsonObj.get("sample").getAsJsonArray();
    List<StructuredRecord> sample = new ArrayList<>();
    jsonArray.iterator().forEachRemaining(jsonElement -> {
      String recordString = jsonElement.getAsString();
      try {
        StructuredRecord record = StructuredRecordStringConverter.fromJsonString(recordString, schema);
        sample.add(record);
      } catch (IOException e) {
        LOG.warn("Error converting the json string {} to StructuredRecord", recordString, e);
      }
    });
    return new SampleResponse(spec, schema, sample);
  }

  @Override
  public JsonElement serialize(SampleResponse sample, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    jsonObj.add("spec", context.serialize(sample.getSpec(), ConnectorSpec.class));
    jsonObj.add("schema", context.serialize(sample.getSchema(), Schema.class));
    JsonArray sampleArray = new JsonArray();
    sample.getSample().forEach(record -> {
      try {
        sampleArray.add(new JsonPrimitive(StructuredRecordStringConverter.toJsonString(record)));
      } catch (IOException e) {
        LOG.warn("Error converting the given record {} to json", record, e);
      }
    });
    jsonObj.add("sample", sampleArray);
    return jsonObj;
  }
}
