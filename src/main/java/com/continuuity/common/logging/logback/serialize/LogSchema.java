package com.continuuity.common.logging.logback.serialize;

import org.apache.avro.Schema;
import org.apache.hadoop.io.MD5Hash;

import java.io.IOException;
import java.net.URL;

/**
 * Handles generation of schema for logging.
 */
public class LogSchema {
  private static final String SCHEMA_LOCATION = "/logging/schema/LoggingEvent.avsc";
  private static final URL SCHEMA_URL = LogSchema.class.getResource(SCHEMA_LOCATION);
  private final Schema schema;
  private final MD5Hash schemaHash;

  public LogSchema() throws IOException {
    this.schema = new Schema.Parser().parse(getClass().getResourceAsStream(SCHEMA_LOCATION));
    this.schemaHash = MD5Hash.digest(getClass().getResourceAsStream(SCHEMA_LOCATION));
  }

  public Schema getAvroSchema() {
    return schema;
  }

  public MD5Hash getSchemaHash() {
    return new MD5Hash(schemaHash.getDigest());
  }

  public static URL getSchemaURL() {
    return SCHEMA_URL;
  }
}
