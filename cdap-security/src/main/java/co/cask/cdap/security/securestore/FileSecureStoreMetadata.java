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

package co.cask.cdap.security.securestore;

import co.cask.cdap.api.security.securestore.SecureStoreMetadata;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
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
    return "FileSecureStoreMetadata{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", created=" + created +
      ", properties=" + properties +
      '}';
  }

  /**
   * Serialize the metadata to a byte array.
   *
   * @return the serialized bytes
   * @throws IOException
   */
  byte[] serialize() throws IOException {
    Gson gson = new Gson();
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(buffer, Charsets.UTF_8);
    gson.toJson(this, out);
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
