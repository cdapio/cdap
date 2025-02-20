/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.storage.spanner;

import com.google.api.client.util.Throwables;
import com.google.cloud.spanner.Struct;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.compression.Compressor;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.storage.spanner.compression.CompressionConfig;
import io.cdap.cdap.storage.spanner.compression.CompressorFactory;
import io.cdap.cdap.storage.spanner.compression.CompressorType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A {@link StructuredRow} implementation backed by GCP Cloud Spanner {@link Struct} object.
 */
public class SpannerStructuredRow implements StructuredRow {

  private final SpannerStructuredTableSchema schema;
  private final Struct struct;
  private volatile Collection<Field<?>> primaryKeys;

  public SpannerStructuredRow(SpannerStructuredTableSchema schema, Struct struct) {
    this.schema = schema;
    this.struct = struct;
  }

  @Nullable
  @Override
  public Integer getInteger(String fieldName) throws InvalidFieldException {
    return isNull(fieldName) ? null : Long.valueOf(struct.getLong(fieldName)).intValue();
  }

  @Nullable
  @Override
  public Long getLong(String fieldName) throws InvalidFieldException {
    return isNull(fieldName) ? null : struct.getLong(fieldName);
  }

  @Nullable
  @Override
  public Boolean getBoolean(String fieldName) throws InvalidFieldException {
    return isNull(fieldName) ? null : struct.getBoolean(fieldName);
  }

  @Nullable
  @Override
  public String getString(String fieldName) throws InvalidFieldException {
    if (isNull(fieldName)) {
      return null;
    }

    String value = struct.getString(fieldName);
    CompressorType compressorType = getCompressorType(fieldName);
    if (value == null || compressorType == null) {
      return value;
    }

    Compressor compressor = CompressorFactory.getCompressor(compressorType);
    try {
      value = compressor.decompress(value);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return value;
  }

  @Nullable
  @Override
  public Float getFloat(String fieldName) throws InvalidFieldException {
    return isNull(fieldName) ? null : Double.valueOf(struct.getDouble(fieldName)).floatValue();
  }

  @Nullable
  @Override
  public Double getDouble(String fieldName) throws InvalidFieldException {
    return isNull(fieldName) ? null : struct.getDouble(fieldName);
  }

  @Nullable
  @Override
  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    if (isNull(fieldName)) {
      return null;
    }

    byte[] value = struct.getBytes(fieldName).toByteArray();
    CompressorType compressorType = getCompressorType(fieldName);
    if (value == null || compressorType == null) {
      return value;
    }

    try {
      Compressor compressor = CompressorFactory.getCompressor(compressorType);
      value = compressor.decompress(value);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return value;
  }

  @Override
  public Collection<Field<?>> getPrimaryKeys() {
    Collection<Field<?>> primaryKeys = this.primaryKeys;
    if (primaryKeys != null) {
      return primaryKeys;
    }

    synchronized (this) {
      primaryKeys = this.primaryKeys;
      if (primaryKeys != null) {
        return primaryKeys;
      }

      primaryKeys = new ArrayList<>();
      for (String key : schema.getPrimaryKeys()) {
        // the NullPointerException should never be thrown since the primary keys must always have a type
        FieldType.Type type = schema.getType(key);
        switch (Objects.requireNonNull(type)) {
          case INTEGER:
            primaryKeys.add(Fields.intField(key, getInteger(key)));
            break;
          case LONG:
            primaryKeys.add(Fields.longField(key, getLong(key)));
            break;
          case STRING:
            primaryKeys.add(Fields.stringField(key, getString(key)));
            break;
          case BYTES:
            primaryKeys.add(Fields.bytesField(key, getBytes(key)));
            break;
          default:
            // this should never happen since all the keys are from the table schema
            // and should never contain other types
            throw new IllegalStateException(
                String.format("The type %s of the primary key %s is not a valid key type", type,
                    key));
        }
      }

      this.primaryKeys = primaryKeys;
      return primaryKeys;
    }
  }

  /**
   * Returns compression status of the field stored in db, if the field is compression enabled.
   */
  private CompressorType getCompressorType(String fieldName) {
    // First check if the field is compression enabled based on cConf.
    if (schema.getCompressorType(fieldName) == null) {
      return null;
    }

    // Use the compression status read from the db.
    String compressorType = getString(fieldName + CompressionConfig.COMPRESSED_COLUMN_SUFFIX);
    return compressorType == null ? null : CompressorType.fromString(compressorType);
  }

  @Override
  public String toString() {
    return "SpannerStructuredRow{"
        + "struct=" + struct
        + '}';
  }

  private boolean isNull(String fieldName) {
    try {
      return struct.isNull(fieldName);
    } catch (IllegalArgumentException e) {
      // If the field is not part of the Struct object, IAE will be thrown.
      // To maintain compatibility with other implementations, any missing field is considered as null.
      return true;
    }
  }
}
