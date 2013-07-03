package com.continuuity.internal.io;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Set;

/**
 *
 */
public final class SchemaHash {

  private final byte[] hash;
  private String hashStr;

  public SchemaHash(Schema schema) {
    hash = computeHash(schema);
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

  private byte[] computeHash(Schema schema) {
    try {
      Set<String> knownRecords = Sets.newHashSet();
      MessageDigest md5 = updateHash(MessageDigest.getInstance("MD5"), schema, knownRecords);
      return md5.digest();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Updates md5 based on the given schema.
   *
   * @param md5 {@link MessageDigest} to update.
   * @param schema {@link Schema} for updating the md5.
   * @param knownRecords bytes to use for updating the md5 for records that're seen before.
   * @return The same {@link MessageDigest} in the parameter.
   */
  private MessageDigest updateHash(MessageDigest md5, Schema schema, Set<String> knownRecords) {
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
          md5.update(Charsets.UTF_8.encode(value));
        }
        break;
      case ARRAY:
        md5.update((byte) 9);
        updateHash(md5, schema.getComponentSchema(), knownRecords);
        break;
      case MAP:
        md5.update((byte) 10);
        updateHash(md5, schema.getMapSchema().getKey(), knownRecords);
        updateHash(md5, schema.getMapSchema().getValue(), knownRecords);
        break;
      case RECORD:
        md5.update((byte) 11);
        boolean notKnown = knownRecords.add(schema.getRecordName());
        for (Schema.Field field : schema.getFields()) {
          md5.update(Charsets.UTF_8.encode(field.getName()));
          if (notKnown) {
            updateHash(md5, field.getSchema(), knownRecords);
          }
        }
        break;
      case UNION:
        md5.update((byte) 12);
        for (Schema unionSchema : schema.getUnionSchemas()) {
          updateHash(md5, unionSchema, knownRecords);
        }
        break;
    }
    return md5;
  }
}
