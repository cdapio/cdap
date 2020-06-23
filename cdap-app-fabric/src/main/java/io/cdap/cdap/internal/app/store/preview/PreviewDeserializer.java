/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store.preview;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.format.io.JsonDecoder;
import io.cdap.cdap.format.io.JsonEncoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumReader;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumWriter;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Type;

/**
 *
 */
public class PreviewDeserializer implements JsonDeserializer<StructuredRecord>, JsonSerializer<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewDeserializer.class);
  private static final JsonStructuredRecordDatumReader JSON_DATUM_READER = new PreviewJsonStructuredRecordDatumReader();
  private static final JsonStructuredRecordDatumWriter JSON_DATUM_WRITER = new PreviewJsonStructuredRecordDatumWriter();

  @Override
  public StructuredRecord deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject asJsonObject = json.getAsJsonObject();

    JsonElement schema = asJsonObject.getAsJsonObject().get("schema");
    JsonElement fields = asJsonObject.getAsJsonObject().get("fields");

    Gson gson = new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
    Schema schemaToRead = gson.fromJson(schema, Schema.class);
    try (JsonReader reader = new JsonReader(new StringReader(fields.getAsString()))) {
      return JSON_DATUM_READER.read(new JsonDecoder(reader), schemaToRead);
    } catch (IOException e) {
      LOG.error("Error while reading structured record {}", e.getMessage(), e);
    }
    return null;
  }

  @Override
  public JsonElement serialize(StructuredRecord src, Type typeOfSrc, JsonSerializationContext context) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter writer = new JsonWriter(strWriter)) {
      JSON_DATUM_WRITER.encode(src, new JsonEncoder(writer));
      return new JsonParser().parse(strWriter.toString()).getAsJsonObject();
    } catch (IOException e) {
      LOG.error("Error while reading structured record {}", e.getMessage(), e);
    }
    return null;
  }
}
