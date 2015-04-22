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

package co.cask.cdap.templates.etl.batch.sinks;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;

/**
 * A {@link co.cask.cdap.templates.etl.api.batch.BatchSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link CubeSink} takes in {@link StructuredRecord} in, maps it to a {@link CubeFact} using mapping configuration
 * provided with {@link #MAPPING_CONFIG_PROPERTY} property, and writes it to a {@link Cube} dataset identified by
 * {@link #NAME} property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 *
 * To configure transformation from {@link StructuredRecord} to a {@link CubeFact} transformation the
 * mapping configuration is required that can be provided in JSON form in {@link #MAPPING_CONFIG_PROPERTY} property.
 * Example of the configuration:
<pre>
  {
    timestamp: {
      sourceField: "timeField",
      sourceFieldFormat: "HH:mm:ss"
    }

    tags: [
      {
        name: "tag1",
        sourceField: "field1"
      },
      {
        name: "tag2",
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
 * In general, the value for a timestamp, tag or measurement fields can be retrieved from {@link StructuredRecord}
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
// todo: add unit-test once CDAP-2156 is resolved
public class CubeSink extends BatchWritableSink<StructuredRecord, byte[], CubeFact> {
  public static final String MAPPING_CONFIG_PROPERTY = "mapping.config";

  private StructuredRecordToCubeFact transform;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("CubeSink");
    configurer.setDescription("CDAP Cube Dataset Batch Sink");
    configurer.addProperty(new Property(NAME, "Name of the Cube dataset. If the Cube does not already exist," +
      " one will be created.", true));
    configurer.addProperty(new Property(Cube.PROPERTY_RESOLUTIONS,
                                        "Aggregation resolutions. See Cube dataset " +
                                          "configuration details for more information", true));
    configurer.addProperty(new Property(MAPPING_CONFIG_PROPERTY,
                                        "the StructuredRecord to CubeFact mapping configuration.", true));
  }

  @Override
  public void initialize(ETLStage stageConfig) throws Exception {
    super.initialize(stageConfig);
    String configAsString = stageConfig.getProperties().get(MAPPING_CONFIG_PROPERTY);
    Preconditions.checkArgument(configAsString != null && !configAsString.isEmpty(),
                                "the mapping config must be given with " + MAPPING_CONFIG_PROPERTY + "property");
    StructuredRecordToCubeFact.MappingConfig config =
      new Gson().fromJson(configAsString, StructuredRecordToCubeFact.MappingConfig.class);

    transform = new StructuredRecordToCubeFact(config);
  }

  @Override
  protected String getDatasetType(ETLStage config) {
    return Cube.class.getName();
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], CubeFact>> emitter) throws Exception {
    emitter.emit(new KeyValue<byte[], CubeFact>(null, transform.transform(input)));
  }
}
