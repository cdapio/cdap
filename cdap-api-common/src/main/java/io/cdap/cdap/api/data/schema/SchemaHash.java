/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import io.cdap.cdap.api.annotation.Beta;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Set;

/**
 * A MD5 hash of a {@link Schema}.
 */
@Beta
public final class SchemaHash implements Serializable {

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final long serialVersionUID = 3640799184470835328L;

  private final byte[] hash;
  private String hashStr;

  public SchemaHash(Schema schema) {
    this(schema, true);
  }

  public SchemaHash(Schema schema, boolean includeRecordName) {
    hash = computeHash(schema, includeRecordName);
  }

  /**
   * Creates a SchemaHash by reading the hash value from the given {@link ByteBuffer}.
   * The position of the {@link ByteBuffer} will be moved to the byte after the hash
   * value.
   */
  public SchemaHash(ByteBuffer bytes) {
    hash = new byte[16];
    bytes.get(hash);
  }

  /**
   * @return A new byte array representing the raw hash value.
   */
  public byte[] toByteArray() {
    return Arrays.copyOf(hash, hash.length);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    return Arrays.equals(hash, ((SchemaHash) other).hash);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(hash);
  }

  @Override
  public String toString() {
    String str = hashStr;
    if (str == null) {
      // hex encode the bytes
      Formatter formatter = new Formatter(new StringBuilder(32));
      for (byte b : hash) {
        formatter.format("%02X", b);
      }
      str = hashStr = formatter.toString();
    }
    return str;
  }

  private byte[] computeHash(Schema schema, boolean includeRecordName) {
    try {
      Set<String> knownRecords = new HashSet<>();
      MessageDigest md5 = updateHash(MessageDigest.getInstance("MD5"), schema, knownRecords, includeRecordName);
      return md5.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Updates md5 based on the given schema.
   *
   * @param md5 {@link MessageDigest} to update.
   * @param schema {@link Schema} for updating the md5.
   * @param knownRecords bytes to use for updating the md5 for records that're seen before.
   * @param includeRecordName whether to include the record name in the hash.
   * @return The same {@link MessageDigest} in the parameter.
   */
  private MessageDigest updateHash(MessageDigest md5, Schema schema, Set<String> knownRecords,
                                   boolean includeRecordName) {
    // Don't use enum.ordinal() as ordering in enum could change
    switch (schema.getType()) {
      case NULL:
        md5.update((byte) 0);
        break;
      case BOOLEAN:
        md5.update((byte) 1);
        break;
      case INT:
        md5.update((byte) 2);
        break;
      case LONG:
        md5.update((byte) 3);
        break;
      case FLOAT:
        md5.update((byte) 4);
        break;
      case DOUBLE:
        md5.update((byte) 5);
        break;
      case BYTES:
        md5.update((byte) 6);
        break;
      case STRING:
        md5.update((byte) 7);
        break;
      case ENUM:
        md5.update((byte) 8);
        for (String value : schema.getEnumValues()) {
          md5.update(UTF_8.encode(value));
        }
        break;
      case ARRAY:
        md5.update((byte) 9);
        updateHash(md5, schema.getComponentSchema(), knownRecords, includeRecordName);
        break;
      case MAP:
        md5.update((byte) 10);
        updateHash(md5, schema.getMapSchema().getKey(), knownRecords, includeRecordName);
        updateHash(md5, schema.getMapSchema().getValue(), knownRecords, includeRecordName);
        break;
      case RECORD:
        md5.update((byte) 11);
        if (includeRecordName) {
          md5.update(UTF_8.encode(schema.getRecordName()));
        }
        boolean notKnown = knownRecords.add(schema.getRecordName());
        for (Schema.Field field : schema.getFields()) {
          md5.update(UTF_8.encode(field.getName()));
          if (notKnown) {
            updateHash(md5, field.getSchema(), knownRecords, includeRecordName);
          }
        }
        break;
      case UNION:
        md5.update((byte) 12);
        for (Schema unionSchema : schema.getUnionSchemas()) {
          updateHash(md5, unionSchema, knownRecords, includeRecordName);
        }
        break;
    }

    Schema.LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          md5.update((byte) 13);
          break;
        case TIME_MILLIS:
          md5.update((byte) 14);
          break;
        case TIME_MICROS:
          md5.update((byte) 15);
          break;
        case TIMESTAMP_MILLIS:
          md5.update((byte) 16);
          break;
        case TIMESTAMP_MICROS:
          md5.update((byte) 17);
          break;
        case DECIMAL:
          md5.update((byte) 18);
          md5.update((byte) schema.getPrecision());
          md5.update((byte) schema.getScale());
          break;
      }
    }

    return md5;
  }
}
