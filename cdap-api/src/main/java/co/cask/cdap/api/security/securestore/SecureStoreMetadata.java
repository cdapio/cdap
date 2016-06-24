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

package co.cask.cdap.api.security.securestore;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the metadata for the data stored in the Secure Store.
 */
public final class SecureStoreMetadata {

  private static final String NAME_FIELD = "name";
  private static final String DESCRIPTION_FIELD = "description";
  private static final String CREATED_FIELD = "created";
  private static final String PROPERTIES_FIELD = "properties";
  private static final String DESCRIPTION_DEFAULT = "";
  private static final Gson GSON = new Gson();

  private final String name;
  private final String description;
  private final Date created;
  private final Map<String, String> properties;

  private SecureStoreMetadata(String name, String description, Date created, Map<String, String> properties) {
    this.name = name;
    this.description = description;
    this.created = created;
    this.properties = properties;
  }

  public static SecureStoreMetadata of(String name, Map<String, String> properties) {
    String tempDescription = properties.get(DESCRIPTION_FIELD) == null ?
      DESCRIPTION_DEFAULT : properties.get(DESCRIPTION_FIELD);

    return new SecureStoreMetadata(name, tempDescription, new Date(), properties);
  }

  /**
   * @return Name of the data.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Last time, in epoch, this element was modified.
   */
  public long getLastModifiedTime() {
    return created.getTime();
  }

  /**
   * @return A map of properties associated with this element.
   */
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
    return "SecureStoreMetadata{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", created=" + created +
      ", properties=" + properties +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SecureStoreMetadata that = (SecureStoreMetadata) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!description.equals(that.description)) {
      return false;
    }
    if (!created.equals(that.created)) {
      return false;
    }
    return properties != null ? properties.equals(that.properties) : that.properties == null;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + description.hashCode();
    result = 31 * result + created.hashCode();
    result = 31 * result + (properties != null ? properties.hashCode() : 0);
    return result;
  }

  /**
   * Serialize the metadata to a byte array.
   *
   * @return the serialized bytes
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(buffer, Charset.forName("UTF-8"));
    GSON.toJson(this, out);
    out.flush();
    return buffer.toByteArray();
  }

  /**
   * Deserialize a new metadata object from a byte array.
   *
   * @param bytes the serialized metadata
   * @throws IOException
   */
  public SecureStoreMetadata(byte[] bytes) throws IOException {
    String name = null;
    Date created = null;
    String description = null;
    Map<String, String> properties = null;
    try (JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream(bytes),
                                                                  Charset.forName("UTF-8")))) {
      reader.beginObject();
      while (reader.hasNext()) {
        String field = reader.nextName();
        if (NAME_FIELD.equals(field)) {
          name = reader.nextString();
        } else if (CREATED_FIELD.equals(field)) {
          created = new Date(reader.nextString());
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
