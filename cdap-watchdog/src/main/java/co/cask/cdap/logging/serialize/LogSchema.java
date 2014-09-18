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

package co.cask.cdap.logging.serialize;

import org.apache.avro.Schema;
import org.apache.hadoop.io.MD5Hash;

import java.io.IOException;
import java.net.URL;

/**
 * Handles generation of schema for logging.
 */
public final class LogSchema {
  private static final String SCHEMA_LOCATION = "/logging/schema/LoggingEvent.avsc";
  private static final URL SCHEMA_URL = LogSchema.class.getResource(SCHEMA_LOCATION);
  private final Schema schema;
  private final SchemaHash schemaHash;

  public LogSchema() throws IOException {
    this.schema = new Schema.Parser().parse(getClass().getResourceAsStream(SCHEMA_LOCATION));
    this.schemaHash = new SchemaHash(MD5Hash.digest(getClass().getResourceAsStream(SCHEMA_LOCATION)));
  }

  public Schema getAvroSchema() {
    return schema;
  }

  public SchemaHash getSchemaHash() {
    return schemaHash;
  }

  public static URL getSchemaURL() {
    return SCHEMA_URL;
  }

  /**
   * Used to generate hash for schema.
   */
  public static final class SchemaHash {
    private final MD5Hash md5Hash;

    public SchemaHash(MD5Hash md5Hash) {
      this.md5Hash = md5Hash;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SchemaHash that = (SchemaHash) o;
      return !(md5Hash != null ? !md5Hash.equals(that.md5Hash) : that.md5Hash != null);
    }

    @Override
    public int hashCode() {
      return md5Hash != null ? md5Hash.hashCode() : 0;
    }

    @Override
    public String toString() {
      return md5Hash.toString();
    }
  }
}
