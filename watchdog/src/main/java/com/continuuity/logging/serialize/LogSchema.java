/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.logging.serialize;

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
