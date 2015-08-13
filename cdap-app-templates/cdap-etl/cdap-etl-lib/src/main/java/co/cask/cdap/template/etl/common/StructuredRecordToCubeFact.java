/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.common;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.dataset.lib.cube.MeasureType;
import co.cask.cdap.api.dataset.lib.cube.Measurement;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transforms a {@link StructuredRecord} into a {@link CubeFact} object that can be written to a
 * {@link co.cask.cdap.api.dataset.lib.cube.Cube} dataset.
 * <p/>
 * To configure transformation from {@link StructuredRecord} to a {@link CubeFact} the
 * mapping configuration can be provided for timestamp field and measurements. All fields from {@link StructuredRecord}
 * will be mapped to corresponded dimensions.
 * <p/>
 * The source field for the timestamp can be provided with {@link Properties.Cube#FACT_TS_FIELD} property.
 * The value in the source field is assumed to be an epoch in milliseconds. For other formats you can use
 * {@link Properties.Cube#FACT_TS_FORMAT} property to specify the date format according to {@link SimpleDateFormat}
 * rules. If no {@link Properties.Cube##FACT_TS_FIELD} is provided, the current timestamp (at processing) is used.
 * <p/>
 * To add a measurement to a {@link CubeFact} specify its type with the property
 * cubeFact.measurement.{@literal<}measurement_name>={@literal<}measurement_type>. Measurement name corresponds to a
 * field name in the {@link StructuredRecord} that contains its value.
 * Measurement type (specified in 'type' property) can be one of {@link co.cask.cdap.api.dataset.lib.cube.MeasureType}
 * values.
 * <p/>
 * Example of the configuration:
<pre>
 cubeFact.timestamp.field=timeField
 cubeFact.timestamp.format=HH:mm:ss
 cubeFact.measurement.metric1=COUNTER
 cubeFact.measurement.metric2=GAUGE
</pre>
 */
public class StructuredRecordToCubeFact {
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final CubeFactBuilder factBuilder;

  public StructuredRecordToCubeFact(Map<String, String> properties) {
    factBuilder = new CubeFactBuilder(properties);
  }

  public CubeFact transform(StructuredRecord record) throws Exception {
    Schema recordSchema = record.getSchema();
    Preconditions.checkArgument(recordSchema.getType() == Schema.Type.RECORD, "input must be a record.");
    return factBuilder.build(record);
  }

  private static final class CubeFactBuilder {
    private final TimestampResolver timestampResolver;
    private final Collection<MeasurementResolver> measurementResolvers;

    public CubeFactBuilder(Map<String, String> properties) {
      Map<String, String> props = new HashMap<>(properties);
      this.timestampResolver = new TimestampResolver(props);
      this.measurementResolvers = Lists.newArrayList();
      addMeasurementsFromProperty(props);
      for (Map.Entry<String, String> property : props.entrySet()) {
        if (property.getKey().startsWith(Properties.Cube.MEASUREMENT_PREFIX)) {
          measurementResolvers.add(new MeasurementResolver(property.getKey(), property.getValue()));
        }
      }
      if (measurementResolvers.isEmpty()) {
        throw new IllegalArgumentException("At least one measurement must be specified with " +
                                             Properties.Cube.MEASUREMENT_PREFIX +
                                             "<measurement_name>=<measurement_type>");
      }
    }

    // todo: workaround for CDAP-2944: allows specifying custom props via JSON map in a property value
    private void addMeasurementsFromProperty(Map<String, String> props) {
      if (props.containsKey(Properties.Cube.MEASUREMENTS)) {
        Map<String, String> measurements = GSON.fromJson(props.get(Properties.Cube.MEASUREMENTS), STRING_MAP_TYPE);
        props.putAll(measurements);
      }
    }

    public CubeFact build(StructuredRecord record) {
      // we divide by 1000 to get seconds - which is expected by Cube
      CubeFact fact = new CubeFact(timestampResolver.getTimestamp(record) / 1000);
      addMeasurements(record, fact);

      for (Schema.Field field : record.getSchema().getFields()) {
        Object value = record.get(field.getName());
        if (value != null) {
          String stringValue = getStringValue(field, value);
          if (stringValue != null) {
            fact.addDimensionValue(field.getName(), stringValue);
          }
        }
      }

      return fact;
    }

    private void addMeasurements(StructuredRecord record, CubeFact fact) {
      for (MeasurementResolver resolver : measurementResolvers) {
        Measurement measurement = resolver.getMeasurement(record);
        if (measurement != null) {
          fact.addMeasurement(measurement);
        }
      }
    }

    @Nullable
    private String getStringValue(Schema.Field field, Object value) {
      Schema.Type type = validateAndGetType(field);
      if (type == null) {
        return null;
      }
      String dimValue;
      switch (type) {
        case BYTES:
          if (value instanceof ByteBuffer) {
            dimValue = Bytes.toString((ByteBuffer) value);
          } else {
            dimValue = Bytes.toStringBinary((byte[]) value);
          }
          break;
        default:
          dimValue = value.toString();
      }

      return dimValue;
    }
  }

  private static final class TimestampResolver {
    private final String srcField;
    private final DateFormat dateFormat;

    public TimestampResolver(Map<String, String> properties) {
      if (properties.containsKey(Properties.Cube.FACT_TS_FIELD)) {
        this.srcField = properties.get(Properties.Cube.FACT_TS_FIELD);
        if (properties.containsKey(Properties.Cube.FACT_TS_FORMAT)) {
          this.dateFormat = new SimpleDateFormat(properties.get(Properties.Cube.FACT_TS_FORMAT));
        } else {
          this.dateFormat = null;
        }
      } else {
        this.srcField = null;
        this.dateFormat = null;
      }
    }

    public long getTimestamp(StructuredRecord record) {
      if (srcField == null) {
        return System.currentTimeMillis();
      }
      Object val = record.get(srcField);
      if (val == null) {
        throw new IllegalArgumentException("Required field to determine timestamp is missing: " + srcField);
      }
      String valAsString = val.toString();
      if (dateFormat != null) {
        try {
          return dateFormat.parse(valAsString).getTime();
        } catch (ParseException e) {
          throw new IllegalArgumentException("Cannot parse field value to determine timestamp: " + valAsString, e);
        }
      }
      return Long.valueOf(valAsString);
    }
  }

  @Nullable
  private static Schema.Type validateAndGetType(Schema.Field field) {
    Schema.Type type;
    if (field.getSchema().isNullable()) {
      type = field.getSchema().getNonNullable().getType();
    } else {
      type = field.getSchema().getType();
    }
    // We only know how to convert simple types into String. Skipping the rest.
    if (!type.isSimpleType()) {
      return null;
    }

    return type;
  }

  private static final class MeasurementResolver {
    private String name;
    private MeasureType type;

    public MeasurementResolver(String measureProperty, String measureType) {
      String measureName = measureProperty.substring(Properties.Cube.MEASUREMENT_PREFIX.length());
      if ("".equals(measureName)) {
        throw new IllegalArgumentException(
          "Invalid property: " + measureProperty + ", measureName must be not empty");
      }
      if (Strings.isNullOrEmpty(measureType)) {
        throw new IllegalArgumentException(
          "Invalid property: " + measureProperty + ", measureType must be not empty");
      }

      this.name = measureName;
      this.type = MeasureType.valueOf(measureType);
    }

    @Nullable
    public Measurement getMeasurement(StructuredRecord record) {
      Long value = getValue(record);
      if (value == null) {
        return null;
      }
      return new Measurement(name, type, value);
    }

    private Long getValue(StructuredRecord record) {
      Object val = record.get(name);
      if (val != null) {
        return Double.valueOf(val.toString()).longValue();
      }

      return null;
    }
  }
}
