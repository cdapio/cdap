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


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.stream.JsonWriter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.format.io.JsonEncoder;
import io.cdap.cdap.format.io.JsonStructuredRecordDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Type;

/**
 * Preview structured record serializer.
 */
public class PreviewJsonSerializer implements JsonSerializer<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewJsonSerializer.class);
  private static final JsonStructuredRecordDatumWriter JSON_DATUM_WRITER = new JsonStructuredRecordDatumWriter(true);

  @Override
  public JsonElement serialize(StructuredRecord src, Type typeOfSrc, JsonSerializationContext context) {
    StringWriter strWriter = new StringWriter();
    try (JsonWriter writer = new JsonWriter(strWriter)) {
      // serialize schema
      JsonObject jsonObj = new JsonObject();
      jsonObj.add("schema", context.serialize(src.getSchema()));
      JsonEncoder encoder = new JsonEncoder(writer);
      JSON_DATUM_WRITER.encode(src, encoder);
      jsonObj.add("fields", new JsonParser().parse(strWriter.toString()));
      return jsonObj;
    } catch (IOException e) {
      LOG.error("Error while serializing structured record {}", e.getMessage(), e);
    }
    return null;
  }
}
