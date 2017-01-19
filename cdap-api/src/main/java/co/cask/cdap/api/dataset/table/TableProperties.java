/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.table;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.ExploreProperties;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Useful class for creating and querying dataset properties for tables.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class TableProperties {

  private static final byte[] DEFAULT_COLUMN_FAMILY_BYTES = Bytes.toBytes(Table.DEFAULT_COLUMN_FAMILY);

  /**
   * Set a conflict detection level in dataset properties.
   */
  public static void setConflictDetection(DatasetProperties.Builder builder, ConflictDetection level) {
    builder.add(Table.PROPERTY_CONFLICT_LEVEL, level.name());
  }

  /**
   * @return the conflict detection level from the properties if present, otherwise the defaultLevel.
   *
   * @throws IllegalArgumentException if the property value is not a valid conflict detection level.
   */
  @Nullable
  public static ConflictDetection getConflictDetection(DatasetProperties props,
                                                       @Nullable ConflictDetection defaultLevel) {
    return getConflictDetection(props.getProperties(), defaultLevel);
  }

  /**
   * @return the conflict detection level from the properties if present, otherwise the defaultLevel.
   *
   * @throws IllegalArgumentException if the property value is not a valid conflict detection level.
   */
  @Nullable
  public static ConflictDetection getConflictDetection(Map<String, String> props,
                                                       @Nullable ConflictDetection defaultLevel) {
    String value = props.get(Table.PROPERTY_CONFLICT_LEVEL);
    if (value == null) {
      return defaultLevel;
    }
    try {
      return ConflictDetection.valueOf(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid conflict detection level: " + value);
    }
  }

  /**
   * Configure read-less increment support in a table's dataset properties.
   */
  public static void setReadlessIncrementSupport(DatasetProperties.Builder builder, boolean enabled) {
    builder.add(Table.PROPERTY_READLESS_INCREMENT, String.valueOf(enabled));
  }

  /**
   * @return whether the dataset properties enable read-less increments. Defaults to false.
   */
  public static boolean getReadlessIncrementSupport(DatasetProperties props) {
    return getReadlessIncrementSupport(props.getProperties());
  }

  /**
   * @return whether the dataset properties enable read-less increments. Defaults to false.
   */
  public static boolean getReadlessIncrementSupport(Map<String, String> props) {
    return "true".equalsIgnoreCase(props.get(Table.PROPERTY_READLESS_INCREMENT));
  }

  /**
   * Set the table TTL, in seconds, in dataset properties. A zero or negative value means no TTL.
   */
  public static void setTTL(DatasetProperties.Builder builder, long ttl) {
    builder.add(Table.PROPERTY_TTL, ttl);
  }

  /**
   * @return the time-to-live (TTL) of the table, in seconds, or null if no TTL is specified.
   *
   * @throws NumberFormatException if the value is not a valid long.
   */
  @Nullable
  public static Long getTTL(DatasetProperties props) {
    return getTTL(props.getProperties());
  }

  /**
   * @return the time-to-live (TTL) of the table, in seconds, or null if no TTL is specified.
   *
   * @throws NumberFormatException if the value is not a valid long.
   */
  @Nullable
  public static Long getTTL(Map<String, String> props) {
    String stringValue = props.get(Table.PROPERTY_TTL);
    if (stringValue == null) {
      return null;
    }
    long ttl = Long.parseLong(stringValue);
    return ttl > 0L ? ttl : null;
  }

  /**
   * Set the column family in a table's dataset properties.
   */
  public static void setColumnFamily(DatasetProperties.Builder builder, String family) {
    builder.add(Table.PROPERTY_COLUMN_FAMILY, family);
  }

  /**
   * Set the column family in a table's dataset properties.
   */
  public static void setColumnFamily(DatasetProperties.Builder builder, byte[] family) {
    setColumnFamily(builder, Bytes.toString(family));
  }

  /**
   * @return the column family of the table, as specified in the properties, or the default column family.
   */
  public static String getColumnFamily(DatasetProperties props) {
    return getColumnFamily(props.getProperties());
  }

  /**
   * @return the column family of the table, as specified in the properties, or the default column family.
   */
  public static String getColumnFamily(Map<String, String> props) {
    String value = props.get(Table.PROPERTY_COLUMN_FAMILY);
    return value == null ? Table.DEFAULT_COLUMN_FAMILY : value;
  }

  /**
   * @return the column family of the table, as specified in the properties, or the default column family.
   */
  public static byte[] getColumnFamilyBytes(DatasetProperties props) {
    return getColumnFamilyBytes(props.getProperties());
  }

  /**
   * @return the column family of the table, as specified in the properties, or the default column family.
   */
  public static byte[] getColumnFamilyBytes(Map<String, String> props) {
    String value = props.get(Table.PROPERTY_COLUMN_FAMILY);
    return value == null ? DEFAULT_COLUMN_FAMILY_BYTES : Bytes.toBytes(value);
  }

  /**
   * Set the schema in a table's dataset properties.
   */
  public static void setSchema(DatasetProperties.Builder builder, Schema schema) {
    builder.add(DatasetProperties.SCHEMA, schema.toString());
  }

  /**
   * @return the schema, parsed as JSON, from the properties.
   *
   * @throws IllegalArgumentException if the schema cannot be parsed.
   */
  @Nullable
  public static Schema getSchema(DatasetProperties props) {
    return getSchema(props.getProperties());
  }

  /**
   * @return the schema, parsed as JSON, from the properties.
   *
   * @throws IllegalArgumentException if the schema cannot be parsed.
   */
  @Nullable
  public static Schema getSchema(Map<String, String> props) {
    String schemaString = props.get(Table.PROPERTY_SCHEMA);
    try {
      return schemaString == null ? null : Schema.parseJson(schemaString);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + schemaString, e);
    }
  }

  /**
   * Configure which field in the schema is the row key.
   */
  public static void setRowFieldName(DatasetProperties.Builder builder, String name) {
    builder.add(Table.PROPERTY_SCHEMA_ROW_FIELD, name);
  }

  /**
   * @return the field in the schema that is used as the row key.
   */
  @Nullable
  public static String getRowFieldName(DatasetProperties props) {
    return getRowFieldName(props.getProperties());
  }

  /**
   * @return the field in the schema that is used as the row key.
   */
  @Nullable
  public static String getRowFieldName(Map<String, String> props) {
    return props.get(Table.PROPERTY_SCHEMA_ROW_FIELD);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for dataset properties for a Table.
   */
  public static class Builder extends AbstractBuilder<Builder> { }

  /**
   * A builder of dataset properties for a Table.
   *
   * @param <B> the type of the builder.
   */
  public abstract static class AbstractBuilder<B extends AbstractBuilder>
    extends ExploreProperties.AbstractBuilder<B> {

    /**
     * Set the conflict detection level.
     */
    @SuppressWarnings("unchecked")
    public B setConflictDetection(ConflictDetection level) {
      TableProperties.setConflictDetection(this, level);
      return (B) this;
    }

    /**
     * Set the table TTL, in seconds. A zero or negative value means no TTL.
     */
    @SuppressWarnings("unchecked")
    public B setTTL(long ttlSeconds) {
      TableProperties.setTTL(this, ttlSeconds);
      return (B) this;
    }

    /**
     * Set the column family for a table.
     */
    @SuppressWarnings("unchecked")
    public B setColumnFamily(String family) {
      TableProperties.setColumnFamily(this, family);
      return (B) this;
    }

    /**
     * Set the column family for a table.
     */
    @SuppressWarnings("unchecked")
    public B setColumnFamily(byte[] family) {
      TableProperties.setColumnFamily(this, family);
      return (B) this;
    }

    /**
     * Configure read-less increment support.
     */
    @SuppressWarnings("unchecked")
    public B setReadlessIncrementSupport(boolean enabled) {
      TableProperties.setReadlessIncrementSupport(this, enabled);
      return (B) this;
    }

    /**
     * Set the schema of a table.
     */
    @SuppressWarnings("unchecked")
    public B setSchema(Schema schema) {
      TableProperties.setSchema(this, schema);
      return (B) this;
    }

    /**
     * Configure which field in the schema is the row key.
     */
    @SuppressWarnings("unchecked")
    public B setRowFieldName(String name) {
      TableProperties.setRowFieldName(this, name);
      return (B) this;
    }
  }
}
