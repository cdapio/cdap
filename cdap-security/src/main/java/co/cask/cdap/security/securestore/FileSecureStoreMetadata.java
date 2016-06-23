/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.securestore.SecureStoreMetadata;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.Charsets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata for the data stored in the Secure Store.
 */
class FileSecureStoreMetadata implements SecureStoreMetadata {

  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String CREATED_FIELD = "created";
  private static final String PROPERTIES_FIELD = "properties";
  private static final String DESCRIPTION_DEFAULT = "";

  private final String name;
  private final String description;
  private final Date created;
  private final Map<String, String> properties;

  private FileSecureStoreMetadata(String name, String description, Date created, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.properties = properties;
  }

  public static SecureStoreMetadata of(String name, Map<String, String> properties) {
    String tempDescription = properties.get(DESCRIPTION_FIELD) == null ?
      DESCRIPTION_DEFAULT : properties.get(DESCRIPTION_FIELD);

    return new FileSecureStoreMetadata(name, tempDescription, new Date(), properties);
  }

  /**
   * @return Name of the data.
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @return Last time, in epoch, this element was modified.
   */
  @Override
  public long getLastModifiedTime() {
    return created.getTime();
  }

  /**
   * @return A map of properties associated with this element.
   */
  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  public String getDescription() {
    return description;
  }

  public Date getCreated() {
    return created;
  }

  @Override
  public String toString() {
    final StringBuilder metaSB = new StringBuilder();
    metaSB.append("name: ").append(name).append(", ");
    metaSB.append("description: ").append(description).append(", ");
    metaSB.append("created: ").append(created).append(", ");
    metaSB.append("properties: ");
    if ((properties != null) && !properties.isEmpty()) {
      for (Map.Entry<String, String> property : properties.entrySet()) {
        metaSB.append("[");
        metaSB.append(property.getKey());
        metaSB.append("=");
        metaSB.append(property.getValue());
        metaSB.append("], ");
      }
      metaSB.deleteCharAt(metaSB.length() - 2);  // remove last ', '
    } else {
      metaSB.append("null");
    }
    return metaSB.toString();
  }

  /**
   * Serialize the metadata to a byte array.
   *
   * @return the serialized bytes
   * @throws IOException
   */
  byte[] serialize() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (JsonWriter writer = new JsonWriter(
      new OutputStreamWriter(buffer, Charsets.UTF_8))) {
      writer.beginObject();
      if (name != null) {
        writer.name(NAME_FIELD).value(name);
      }
      if (created != null) {
        writer.name(CREATED_FIELD).value(created.getTime());
      }
      if (description != null) {
        writer.name(DESCRIPTION_FIELD).value(description);
      }
      if (properties != null && properties.size() > 0) {
        writer.name(PROPERTIES_FIELD).beginObject();
        for (Map.Entry<String, String> property : properties.entrySet()) {
          writer.name(property.getKey()).value(property.getValue());
        }
        writer.endObject();
      }
      writer.endObject();
      writer.flush();
    }
    return buffer.toByteArray();
  }

  /**
   * Deserialize a new metadata object from a byte array.
   *
   * @param bytes the serialized metadata
   * @throws IOException
   */
  FileSecureStoreMetadata(byte[] bytes) throws IOException {
    String name = null;
    Date created = null;
    String description = null;
    Map<String, String> properties = null;
    try (JsonReader reader = new JsonReader(new InputStreamReader
                                              (new ByteArrayInputStream(bytes), Charsets.UTF_8))) {
      reader.beginObject();
      while (reader.hasNext()) {
        String field = reader.nextName();
        if (NAME_FIELD.equals(field)) {
          name = reader.nextString();
        } else if (CREATED_FIELD.equals(field)) {
          created = new Date(reader.nextLong());
        } else if (DESCRIPTION_FIELD.equals(field)) {
          description = reader.nextString();
        } else if (PROPERTIES_FIELD.equalsIgnoreCase(field)) {
          reader.beginObject();
          properties = new HashMap<>();
          while (reader.hasNext()) {
            properties.put(reader.nextName(), reader.nextString());
          }
          reader.endObject();
        }
      }
      reader.endObject();
    }
    this.name = name;
    this.created = created;
    this.description = description;
    this.properties = properties;
  }
}
