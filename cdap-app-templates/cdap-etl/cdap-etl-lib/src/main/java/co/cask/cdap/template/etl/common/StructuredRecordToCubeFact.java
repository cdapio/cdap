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
import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Transforms a {@link StructuredRecord} into a {@link CubeFact} object that can be written to a
 * {@link co.cask.cdap.api.dataset.lib.cube.Cube} dataset.
 * <p/>
 * To configure transformation from {@link StructuredRecord} to a {@link CubeFact} the
 * mapping configuration is required, that can be provided in JSON form in {@link #MAPPING_CONFIG_PROPERTY} property.
 * Example of the configuration:
<pre>
  {
    timestamp: {
      sourceField: "timeField",
      sourceFieldFormat: "HH:mm:ss"
    }

    dimensions: [
      {
        name: "dim1",
        sourceField: "field1"
      },
      {
        name: "dim2",
        value: "staticValue"
      }
    ],

    measurements: [
      {
        name: "metric1",
        type: "COUNTER",
        sourceField: "field7"
      },
      {
        name: "metric2",
        type: "GAUGE",
        sourceField: "field7"
      },
      {
        name: "metric3",
        type: "COUNTER",
        value: "1"
      },
    ],
  }
</pre>
 *
 * In general, the value for a timestamp, dimension or measurement fields can be retrieved from {@link StructuredRecord}
 * field using 'srcField' property or set to a fixed value using 'value' property. If both are specified, then the one
 * in 'value' is used as the default when 'srcField' is not present in {@link StructuredRecord}.
 *
 * <h3>Special cases</h3>
 * <p/>
 * Timestamp is either retrieved from the record field or is set to a current ts (at processing) is used.
 * If record field is used, either dateFormat is used to parse the value, or it is assumed as epoch in ms.
 * To use current ts, configure it the following way:
 *
<pre>
  {
    timestamp: {
      value: "now"
    },
    ...
  }
 </pre>
 *
 * <p/>
 * Measurement type (specified in 'type' property) can be one of {@link co.cask.cdap.api.dataset.lib.cube.MeasureType}
 * values.
 *
 */
public class StructuredRecordToCubeFact {
  public static final String MAPPING_CONFIG_PROPERTY = "mapping.config";

  private final CubeFactBuilder factBuilder;

  public StructuredRecordToCubeFact(Map<String, String> properties) {
    String configAsString = properties.get(MAPPING_CONFIG_PROPERTY);
    Preconditions.checkArgument(configAsString != null && !configAsString.isEmpty(),
                                "the mapping config must be given with " + MAPPING_CONFIG_PROPERTY + "property");
    StructuredRecordToCubeFact.MappingConfig config =
      new Gson().fromJson(configAsString, StructuredRecordToCubeFact.MappingConfig.class);
    factBuilder = new CubeFactBuilder(config);
  }

  public CubeFact transform(StructuredRecord record) throws Exception {
    Schema recordSchema = record.getSchema();
    Preconditions.checkArgument(recordSchema.getType() == Schema.Type.RECORD, "input must be a record.");
    return factBuilder.build(record);
  }

  private static final class CubeFactBuilder {
    private final TimestampResolver timestampResolver;
    private final Collection<DimensionValueResolver> dimensionResolvers;
    private final Collection<MeasurementResolver> measurementResolvers;

    public CubeFactBuilder(MappingConfig config) {
      this.timestampResolver = new TimestampResolver(config.timestamp);
      if (config.dimensions == null) {
        throw new IllegalArgumentException("Missing required 'dimensions' configuration: " + config);
      }
      this.dimensionResolvers = Lists.newArrayList();
      for (ValueMapping mapping : config.dimensions) {
        dimensionResolvers.add(new DimensionValueResolver(mapping));
      }
      if (config.measurements == null) {
        throw new IllegalArgumentException("Missing required 'measurements' configuration: " + config);
      }
      this.measurementResolvers = Lists.newArrayList();
      for (ValueMapping mapping : config.measurements) {
        measurementResolvers.add(new MeasurementResolver(mapping));
      }
    }

    public CubeFact build(StructuredRecord record) {
      // we divide by 1000 to get seconds - which is expected by Cube
      CubeFact fact = new CubeFact(timestampResolver.getTimestamp(record) / 1000);
      for (DimensionValueResolver resolver : dimensionResolvers) {
        String value = resolver.getValue(record);
        if (value != null) {
          fact.addDimensionValue(resolver.getName(), value);
        }
      }
      for (MeasurementResolver resolver : measurementResolvers) {
        Measurement measurement = resolver.getMeasurement(record);
        if (measurement != null) {
          fact.addMeasurement(measurement);
        }
      }
      return fact;
    }
  }

  private static final class TimestampResolver {
    private String srcField;
    private DateFormat dateFormat;

    public TimestampResolver(ValueMapping valueMapping) {
      if (valueMapping == null) {
        throw new IllegalArgumentException("Missing required configuration for CubeFact timestamp.");
      }
      // Timestamp is either retrieved from the record field or current ts (at processing) is used.
      // If record field is used, either dateFormat is used to parse the value, or it is assumed as epoch in ms
      if (valueMapping.sourceField != null) {
        this.srcField = valueMapping.sourceField;
        if (valueMapping.sourceFieldFormat != null) {
          this.dateFormat = new SimpleDateFormat(valueMapping.sourceFieldFormat);
        }
      } else if (!"now".equals(valueMapping.value)) {
        throw new IllegalArgumentException("Invalid configuration for CubeFact timestamp: " + valueMapping);
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

  private static class DimensionValueResolver {
    protected String name;
    protected String srcField;
    protected String value;

    public DimensionValueResolver(ValueMapping valueMapping) {
      if (valueMapping.name == null) {
        throw new IllegalArgumentException("Required 'name' of a dimension is missing in CubeFact mapping config: " +
                                             valueMapping);
      }
      this.name = valueMapping.name;
      // The value is either retrieved from record field or set as static value.
      if (valueMapping.sourceField != null) {
        this.srcField = valueMapping.sourceField;
      } else if (valueMapping.value == null) {
        throw new IllegalArgumentException("Either 'value' or 'sourceField' must be specified for dimension: " +
                                             valueMapping);
      }
      this.value = valueMapping.value;
    }

    public String getName() {
      return name;
    }

    @Nullable
    public String getValue(StructuredRecord record) {
      if (srcField == null) {
        return value;
      }
      Object val = record.get(srcField);
      if (val != null) {
        Schema recordSchema = record.getSchema();
        Schema.Field field = recordSchema.getField(srcField);
        Schema.Type type = validateAndGetType(field);
        String dimValue;
        switch (type) {
          case BYTES:
            if (val instanceof ByteBuffer) {
              dimValue = Bytes.toString((ByteBuffer) val);
            } else {
              dimValue = Bytes.toStringBinary((byte[]) val);
            }
            break;
          default:
            dimValue = val.toString();
        }

        return dimValue;
      }
      // value is default if srcField is missing
      if (value != null) {
        return value;
      }

      return null;
    }
  }

  private static Schema.Type validateAndGetType(Schema.Field field) {
    Schema.Type type;
    if (field.getSchema().isNullable()) {
      type = field.getSchema().getNonNullable().getType();
    } else {
      type = field.getSchema().getType();
    }
    Preconditions.checkArgument(type.isSimpleType(),
                                "only simple types are supported (boolean, int, long, float, double, bytes).");
    return type;
  }

  private static final class MeasurementResolver {
    private String name;
    private MeasureType type;
    private String srcField;
    private Long value;

    public MeasurementResolver(ValueMapping valueMapping) {
      if (valueMapping.name == null) {
        throw new IllegalArgumentException("Required 'name' of a measurement is missing in CubeFact mapping config: " +
                                             valueMapping);
      }
      this.name = valueMapping.name;
      // The value is either retrieved from record field or set as static value.
      if (valueMapping.sourceField != null) {
        this.srcField = valueMapping.sourceField;
      } else if (valueMapping.value == null) {
        throw new IllegalArgumentException("Either 'value' or 'sourceField' must be specified for measurement" +
                                             valueMapping);
      }
      if (valueMapping.type == null) {
        throw new IllegalArgumentException("Required 'type' value is missing in CubeFact measurement mapping config: " +
                                             valueMapping);
      }
      this.type = MeasureType.valueOf(valueMapping.type);
      try {
        this.value = valueMapping.value == null ? null : Long.valueOf(valueMapping.value);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Cannot parse 'value' of a measurement as long: " + valueMapping.value);
      }
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
      if (srcField == null) {
        return value;
      }
      Object val = record.get(srcField);
      if (val != null) {
        return Double.valueOf(val.toString()).longValue();
      }
      // value is default if srcField is missing
      if (value != null) {
        return value;
      }

      return null;
    }
  }

  // This class is needed to help with JSON serde
  @SuppressWarnings("unused")
  static final class MappingConfig {
    ValueMapping timestamp;
    ValueMapping[] dimensions;
    ValueMapping[] measurements;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("MappingConfig");
      sb.append("{timestamp=").append(timestamp);
      sb.append(", dimensions=").append(dimensions == null ? "null" : Arrays.asList(dimensions).toString());
      sb.append(", measurements=").append(measurements == null ? "null" : Arrays.asList(measurements).toString());
      sb.append('}');
      return sb.toString();
    }
  }

  // This class is needed to help with JSON serde
  @SuppressWarnings("unused")
  static final class ValueMapping {
    String name;
    String type;
    String value;
    String sourceField;
    String sourceFieldFormat;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("ValueMapping");
      sb.append("{name='").append(name).append('\'');
      sb.append(", type='").append(type).append('\'');
      sb.append(", value='").append(value).append('\'');
      sb.append(", sourceField='").append(sourceField).append('\'');
      sb.append(", sourceFieldFormat='").append(sourceFieldFormat).append('\'');
      sb.append('}');
      return sb.toString();
    }
  }
}
