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

import com.google.cloud.spanner.Struct;
import io.cdap.cdap.spi.data.InvalidFieldException;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.table.StructuredTableSchema;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Objects;
import javax.annotation.Nullable;
import org.xerial.snappy.Snappy;

/**
 * A {@link StructuredRow} implementation backed by GCP Cloud Spanner {@link Struct} object.
 */
public class SpannerStructuredRow implements StructuredRow {

  private final StructuredTableSchema schema;
  private final Struct struct;
  private volatile Collection<Field<?>> primaryKeys;

  public SpannerStructuredRow(StructuredTableSchema schema, Struct struct) {
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
    String value = null;
    if (!isNull(fieldName)) {
      value = struct.getString(fieldName);
      String compressor = schema.getCompressor(fieldName);
      if (value != null && compressor != null) {
        if (compressor.equals("snappy")) {
          value = snappyDecompress(fieldName, value);
        } else {
          throw new InvalidFieldException(schema.getTableId(), fieldName,
              "No implementation found for compressor " + compressor);
        }
      }
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
    byte[] value = null;
    if (!isNull(fieldName)) {
      value = struct.getBytes(fieldName).toByteArray();
      String compressor = schema.getCompressor(fieldName);
      if (value != null && compressor != null) {
        if (compressor.equals("snappy")) {
          value = snappyDecompress(fieldName, value);
        } else {
          throw new InvalidFieldException(schema.getTableId(), fieldName,
              "No implementation found for compressor " + compressor);
        }
      }
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

  private String snappyDecompress(String field, String value) throws InvalidFieldException {
    return new String(snappyDecompress(field, Base64.getDecoder().decode(value)),
        StandardCharsets.UTF_8);
  }

  private byte[] snappyDecompress(String field, byte[] value) throws InvalidFieldException {
    byte[] compressedBytes = Base64.getDecoder().decode(value);
    try {
      return Snappy.uncompress(compressedBytes);
    } catch (IOException e) {
      throw new InvalidFieldException(schema.getTableId(), field, "snappy", e);
    }
  }
}
