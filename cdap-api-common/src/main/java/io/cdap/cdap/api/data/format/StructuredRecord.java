/*
 * Copyright © 2014-2018 Cask Data, Inc.
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

package io.cdap.cdap.api.data.format;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Instance of a record structured by a {@link Schema}. Fields are accessible by name.
 */
@Beta
public class StructuredRecord implements Serializable {
  private static final SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss z");
  private final Schema schema;
  private final Map<String, Object> fields;

  private static final long serialVersionUID = -6547770456592865613L;

  {
    DEFAULT_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private StructuredRecord(Schema schema, Map<String, Object> fields) {
    this.schema = schema;
    this.fields = fields;
  }

  /**
   * Get the schema of the record.
   *
   * @return schema of the record.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the value of a field in the record.
   *
   * @param fieldName field to get.
   * @param <T> type of object of the field value.
   * @return value of the field.
   */
  @SuppressWarnings("unchecked")
  @Nullable
  public <T> T get(String fieldName) {
    return (T) fields.get(fieldName);
  }

  /**
   * Get the {@link LocalDate} from field. The field must have {@link LogicalType#DATE} as its logical type.
   *
   * @param fieldName date field to get.
   * @return value of the field as a {@link LocalDate}
   * @throws UnexpectedFormatException if the provided field is not of {@link LogicalType#DATE} type.
   */
  @Nullable
  public LocalDate getDate(String fieldName) {
    Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(schema.getField(fieldName),
                                                               EnumSet.of(LogicalType.DATE));
    Integer value = (Integer) fields.get(fieldName);
    return (value == null || logicalTypeSchema == null) ? null : LocalDate.ofEpochDay(value.longValue());
  }

  /**
   * Get {@link LocalTime} from field. The field must have {@link LogicalType#TIME_MILLIS}
   * or {@link LogicalType#TIME_MICROS} as its logical type.
   *
   * @param fieldName time field to get.
   * @return value of the field as a {@link LocalTime}
   * @throws UnexpectedFormatException if the provided field is not of {@link LogicalType#TIME_MILLIS} or
   *                                   {@link LogicalType#TIME_MICROS} type.
   */
  @Nullable
  public LocalTime getTime(String fieldName) {
    Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(schema.getField(fieldName),
                                                               EnumSet.of(LogicalType.TIME_MILLIS,
                                                                          LogicalType.TIME_MICROS));
    Object value = fields.get(fieldName);
    if (value == null || logicalTypeSchema == null) {
      return null;
    }

    if (logicalTypeSchema.getLogicalType() == LogicalType.TIME_MILLIS) {
      return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(((Integer) value)));
    }

    return LocalTime.ofNanoOfDay(TimeUnit.MICROSECONDS.toNanos((Long) value));
  }

  /**
   * Get the {@link ZonedDateTime} with timezone 'UTC' for the specified field.
   * The field must have {@link Schema.LogicalType#TIMESTAMP_MILLIS} or {@link Schema.LogicalType#TIMESTAMP_MICROS}
   * as its logical type.
   *
   * @param fieldName zoned date time field to get.
   * @return value of the field as a {@link ZonedDateTime}
   * @throws UnexpectedFormatException if the provided field is not of {@link LogicalType#TIMESTAMP_MILLIS} or
   *                                   {@link LogicalType#TIMESTAMP_MICROS} type.
   */
  @Nullable
  public ZonedDateTime getTimestamp(String fieldName) {
    return getTimestamp(fieldName, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  /**
   * Get the {@link ZonedDateTime} with provided timezone.
   * The field must have {@link Schema.LogicalType#TIMESTAMP_MILLIS} or {@link Schema.LogicalType#TIMESTAMP_MICROS}
   * as its logical type.
   *
   * @param fieldName zoned date time field to get.
   * @param zoneId zone id for the field
   * @return value of the field as a {@link ZonedDateTime}
   * @throws UnexpectedFormatException if the provided field is not of {@link LogicalType#TIMESTAMP_MILLIS} or
   *                                   {@link LogicalType#TIMESTAMP_MICROS} type.
   */
  @Nullable
  public ZonedDateTime getTimestamp(String fieldName, ZoneId zoneId) {
    Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(schema.getField(fieldName),
                                                               EnumSet.of(LogicalType.TIMESTAMP_MILLIS,
                                                                          LogicalType.TIMESTAMP_MICROS));
    Object value = fields.get(fieldName);
    if (value == null || logicalTypeSchema == null) {
      return null;
    }

    if (logicalTypeSchema.getLogicalType() == LogicalType.TIMESTAMP_MILLIS) {
      return getZonedDateTime((long) value, TimeUnit.MILLISECONDS, zoneId);
    }

    return getZonedDateTime((long) value, TimeUnit.MICROSECONDS, zoneId);
  }

  /**
   * Get the {@link BigDecimal} from field. The field must have {@link LogicalType#DECIMAL} as its logical type.
   *
   * @param fieldName decimal field to get.
   * @return value of the field as a {@link BigDecimal}
   * @throws UnexpectedFormatException if the provided field is not of {@link LogicalType#DECIMAL} type.
   */
  @Nullable
  public BigDecimal getDecimal(String fieldName) {
    Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(schema.getField(fieldName),
                                                               EnumSet.of(LogicalType.DECIMAL));
    Object value = fields.get(fieldName);
    if (value == null || logicalTypeSchema == null) {
      return null;
    }

    int scale = logicalTypeSchema.getScale();
    if (value instanceof ByteBuffer) {
      return new BigDecimal(new BigInteger(Bytes.toBytes((ByteBuffer) value)), scale);
    }

    return new BigDecimal(new BigInteger((byte[]) value), scale);
  }

  /**
   * Get zoned date and time represented by the field.
   *
   * @param ts timestamp to be used to get {@link ZonedDateTime}
   * @param zoneId zone id for the field
   * @param unit time unit for ts
   * @return {@link ZonedDateTime} represented by field.
   */
  private ZonedDateTime getZonedDateTime(long ts, TimeUnit unit, ZoneId zoneId) {
    long mod = unit.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (ts % mod);
    long tsInSeconds = unit.toSeconds(ts);
    // create an Instant with time in seconds and fraction which will be stored as nano seconds.
    Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, zoneId);
  }

  /**
   * Validates and returns the underlying {@link LogicalType} Schema of the given {@link Schema.Field}.
   *
   * @param field field with logical type
   * @param allowedTypes acceptable logical types
   * @return the Schema of underlying {@link LogicalType} for the field
   * @throws UnexpectedFormatException if the field provided does not have a {@link LogicalType} that is
   *                                   one of the acceptable types
   */
  @Nullable
  private static Schema validateAndGetLogicalTypeSchema(Schema.Field field, Set<LogicalType> allowedTypes) {
    if (field == null) {
      return null;
    }

    String fieldName = field.getName();
    Schema logicalTypeSchema = getLogicalTypeSchema(field.getSchema(), allowedTypes);

    if (logicalTypeSchema == null) {
      throw new UnexpectedFormatException(String.format("Field %s does not have a logical type.", fieldName));
    }

    LogicalType logicalType = logicalTypeSchema.getLogicalType();
    if (!allowedTypes.contains(logicalType)) {
      throw new UnexpectedFormatException(String.format("Field %s must be of logical type %s, instead it is of type %s",
                                                        fieldName, allowedTypes, logicalType));
    }
    return logicalTypeSchema;
  }

  /**
   * Gets the {@link Schema} with logical type.
   * If the given {@link Schema} is a {@link Schema.Type#UNION}, this method will find
   * the schema recursively and return the schema for the first {@link LogicalType} it encountered.
   *
   * @param schema the {@link Schema} for finding the {@link LogicalType}
   * @param allowedTypes acceptable logical types
   * @return the {@link Schema} for logical type or {@code null} if no {@link LogicalType} was found
   */
  @Nullable
  private static Schema getLogicalTypeSchema(Schema schema, Set<LogicalType> allowedTypes) {
    LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null && allowedTypes.contains(logicalType)) {
      return schema;
    }

    if (schema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : schema.getUnionSchemas()) {
        Schema logicalTypeSchema = getLogicalTypeSchema(unionSchema, allowedTypes);

        if (logicalTypeSchema != null) {
          return logicalTypeSchema;
        }
      }
    }
    return null;
  }

  /**
   * Get a builder for creating a record with the given schema.
   *
   * @param schema schema for the record to build.
   * @return builder for creating a record with the given schema.
   * @throws UnexpectedFormatException if the given schema is not a record with at least one field.
   */
  public static Builder builder(Schema schema) throws UnexpectedFormatException {
    if (schema == null || schema.getType() != Schema.Type.RECORD || schema.getFields().size() < 1) {
      throw new UnexpectedFormatException("Schema must be a record with at least one field.");
    }
    return new Builder(schema);
  }

  /**
   * Builder for creating a {@link StructuredRecord}.
   * TODO: enforce schema correctness?
   */
  public static class Builder {
    private final Schema schema;
    private Map<String, Object> fields;

    private Builder(Schema schema) {
      this.schema = schema;
      this.fields = new HashMap<>();
    }

    /**
     * Set the field to the given value.
     *
     * @param fieldName name of the field to set
     * @param value value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     */
    public Builder set(String fieldName, @Nullable Object value) {
      validateAndGetField(fieldName, value);
      fields.put(fieldName, value);
      return this;
    }

    /**
     * Sets the date value for {@link LogicalType#DATE} field
     *
     * @param fieldName name of the field to set
     * @param localDate value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given or if the provided date is an invalid date
     */
    public Builder setDate(String fieldName, @Nullable LocalDate localDate) {
      validateAndGetLogicalTypeSchema(validateAndGetField(fieldName, localDate), EnumSet.of(LogicalType.DATE));
      if (localDate == null) {
        fields.put(fieldName, null);
        return this;
      }
      try {
        fields.put(fieldName, Math.toIntExact(localDate.toEpochDay()));
      } catch (ArithmeticException e) {
        // Highest integer is 2,147,483,647 which is Jan 1 2038.
        throw new UnexpectedFormatException(String.format("Field %s was set to a date that is too large." +
                                                            "Valid date should be below Jan 1 2038", fieldName));
      }
      return this;
    }

    /**
     * Sets the time value for {@link LogicalType#TIME_MILLIS} or {@link LogicalType#TIME_MICROS} field
     *
     * @param fieldName name of the field to set
     * @param localTime value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given
     */
    public Builder setTime(String fieldName, @Nullable LocalTime localTime) {
      Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(validateAndGetField(fieldName, localTime),
                                                                 EnumSet.of(LogicalType.TIME_MILLIS,
                                                                            LogicalType.TIME_MICROS));

      if (localTime == null) {
        fields.put(fieldName, null);
        return this;
      }

      long nanos = localTime.toNanoOfDay();

      if (logicalTypeSchema.getLogicalType() == LogicalType.TIME_MILLIS) {
        try {
          int millis = Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(nanos));
          fields.put(fieldName, millis);
        } catch (ArithmeticException e) {
          throw new UnexpectedFormatException(String.format("Field %s was set to a time that is too large.",
                                                            fieldName));
        }
        return this;
      }

      long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
      fields.put(fieldName, micros);
      return this;
    }

    /**
     * Sets the timestamp value for {@link LogicalType#TIMESTAMP_MILLIS} or
     * {@link LogicalType#TIMESTAMP_MICROS} field
     *
     * @param fieldName name of the field to set
     * @param zonedDateTime value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given or if the provided date is an invalid timestamp
     */
    public Builder setTimestamp(String fieldName, @Nullable ZonedDateTime zonedDateTime) {
      Schema logicalTypeSchema = validateAndGetLogicalTypeSchema(validateAndGetField(fieldName, zonedDateTime),
                                                                 EnumSet.of(LogicalType.TIMESTAMP_MILLIS,
                                                                            LogicalType.TIMESTAMP_MICROS));

      if (zonedDateTime == null) {
        fields.put(fieldName, null);
        return this;
      }

      Instant instant = zonedDateTime.toInstant();
      try {
        if (logicalTypeSchema.getLogicalType() == LogicalType.TIMESTAMP_MILLIS) {
          long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond());
          long tsMillis = Math.addExact(millis, TimeUnit.NANOSECONDS.toMillis(instant.getNano()));
          fields.put(fieldName, tsMillis);
          return this;
        }

        long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
        long tsMicros = Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
        fields.put(fieldName, tsMicros);
        return this;
      } catch (ArithmeticException e) {
        throw new UnexpectedFormatException(String.format("Field %s was set to a timestamp that is too large.",
                                                          fieldName));
      }
    }

    /**
     * Sets the decimal value for provided {@link LogicalType#DECIMAL} field. The decimal value precision should be
     * less than or equal to precision provided in {@link Schema.LogicalType} and the scale should be exactly same.
     *
     * @param fieldName name of the field to set
     * @param decimal value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given or if the provided decimal is invalid
     */
    public Builder setDecimal(String fieldName, @Nullable BigDecimal decimal) {
      Schema logicalSchema = validateAndGetLogicalTypeSchema(validateAndGetField(fieldName, decimal),
                                                             EnumSet.of(LogicalType.DECIMAL));
      if (decimal == null) {
        fields.put(fieldName, null);
        return this;
      }

      if (decimal.precision() > logicalSchema.getPrecision()) {
        throw new UnexpectedFormatException(
          String.format("Field '%s' has precision '%s' which is higher than schema precision '%s'.",
                        fieldName, decimal.precision(), logicalSchema.getPrecision()));
      }

      if (decimal.scale() != logicalSchema.getScale()) {
        throw new UnexpectedFormatException(
          String.format("Field '%s' has scale '%s' which is not equal to schema scale '%s'.",
                        fieldName, decimal.scale(), logicalSchema.getScale()));
      }

      fields.put(fieldName, decimal.unscaledValue().toByteArray());
      return this;
    }


    /**
     * Convert the given date into the type of the given field, and set the value for that field.
     * A Date can be converted into a long or a string.
     * This method does not support {@link LogicalType#DATE}
     *
     * @deprecated As of release 5.1.0, use {@link StructuredRecord.Builder#setDate(String, LocalDate)} instead.
     *
     * @param fieldName name of the field to set
     * @param date date value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the date cannot be converted to the type for the field
     */
    @Deprecated
    public Builder convertAndSet(String fieldName, @Nullable Date date) throws UnexpectedFormatException {
      return convertAndSet(fieldName, date, DEFAULT_FORMAT);
    }

    /**
     * Convert the given date into the type of the given field, and set the value for that field.
     * A Date can be converted into a long or a string, using a date format, if supplied.
     * This method does not support {@link LogicalType#DATE}
     *
     * @deprecated As of release 5.1.0, use {@link StructuredRecord.Builder#setDate(String, LocalDate)} instead.
     *
     * @param fieldName name of the field to set
     * @param date date value for the field
     * @param dateFormat format for the date if it is a string. If null, the default format of
     *                   "YYYY-MM-DD'T'HH:mm:ss z" will be used
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the date cannot be converted to the type for the field
     */
    @Deprecated
    public Builder convertAndSet(String fieldName, @Nullable Date date,
                                 @Nullable DateFormat dateFormat) throws UnexpectedFormatException {
      Schema.Field field = validateAndGetField(fieldName, date);
      boolean isNullable = field.getSchema().isNullable();
      if (isNullable && date == null) {
        fields.put(fieldName, null);
        return this;
      }

      Schema.Type fieldType = isNullable ? field.getSchema().getNonNullable().getType() : field.getSchema().getType();
      if (fieldType == Schema.Type.LONG) {
        fields.put(fieldName, date.getTime());
      } else if (fieldType == Schema.Type.STRING) {
        DateFormat format = dateFormat == null ? DEFAULT_FORMAT : dateFormat;
        fields.put(fieldName, format.format(date));
      } else {
        throw new UnexpectedFormatException("Date must be either a long or a string, not a " + fieldType);
      }
      return this;
    }

    /**
     * Convert the given string into the type of the given field, and set the value for that field. A String can be
     * converted to a boolean, int, long, float, double, bytes, string, or null.
     *
     * @param fieldName name of the field to set
     * @param strVal string value for the field
     * @return this builder
     * @throws UnexpectedFormatException if the field is not in the schema, or the field is not nullable but a null
     *                                   value is given, or the string cannot be converted to the type for the field
     */
    public Builder convertAndSet(String fieldName, @Nullable String strVal) throws UnexpectedFormatException {
      Schema.Field field = validateAndGetField(fieldName, strVal);
      fields.put(fieldName, convertString(field.getSchema(), strVal));
      return this;
    }

    /**
     * Build a {@link StructuredRecord} with the fields set by this builder.
     *
     * @return a {@link StructuredRecord} with the fields set by this builder
     * @throws UnexpectedFormatException if there is at least one non-nullable field without a value
     */
    public StructuredRecord build() throws UnexpectedFormatException {
      // check that all non-nullable fields have a value.
      for (Schema.Field field : schema.getFields()) {
        String fieldName = field.getName();
        if (!fields.containsKey(fieldName)) {
          // if the field is not nullable and there is no value set for the field, this is invalid.
          if (!field.getSchema().isNullable()) {
            throw new UnexpectedFormatException("Field " + fieldName + " must contain a value.");
          } else {
            // otherwise, set the value for the field to null
            fields.put(fieldName, null);
          }
        }
      }
      return new StructuredRecord(schema, fields);
    }

    private Object convertString(Schema schema, String strVal) throws UnexpectedFormatException {
      Schema.Type simpleType;
      if (schema.getType().isSimpleType()) {
        simpleType = schema.getType();
        if (strVal == null && simpleType != Schema.Type.NULL) {
          throw new UnexpectedFormatException("Cannot set non-nullable field to a null value.");
        }
      } else if (schema.isNullable()) {
        if (strVal == null) {
          return null;
        }
        simpleType = schema.getNonNullable().getType();
      } else {
        throw new UnexpectedFormatException("Cannot convert a string to schema " + schema);
      }

      switch (simpleType) {
        case BOOLEAN:
          return Boolean.parseBoolean(strVal);
        case INT:
          return Integer.parseInt(strVal);
        case LONG:
          return Long.parseLong(strVal);
        case FLOAT:
          return Float.parseFloat(strVal);
        case DOUBLE:
          return Double.parseDouble(strVal);
        case BYTES:
          return Bytes.toBytesBinary(strVal);
        case STRING:
          return strVal;
        case NULL:
          return null;
        default:
          // shouldn't ever get here
          throw new UnexpectedFormatException("Cannot convert a string to schema " + schema);
      }
    }

    private Schema.Field validateAndGetField(String fieldName, Object val) {
      Schema.Field field = schema.getField(fieldName);
      if (field == null) {
        throw new UnexpectedFormatException("field " + fieldName + " is not in the schema.");
      }
      Schema fieldSchema = field.getSchema();
      if (val == null) {
        if (fieldSchema.getType() == Schema.Type.NULL) {
          return field;
        }
        if (fieldSchema.getType() != Schema.Type.UNION) {
          throw new UnexpectedFormatException("field " + fieldName + " cannot be set to a null value.");
        }
        for (Schema unionSchema : fieldSchema.getUnionSchemas()) {
          if (unionSchema.getType() == Schema.Type.NULL) {
            return field;
          }
        }
        throw new UnexpectedFormatException("field " + fieldName + " cannot be set to a null value.");
      }
      return field;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StructuredRecord that = (StructuredRecord) o;

    return Objects.equals(schema, that.schema) && Objects.equals(fields, that.fields);

  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, fields);
  }
}
