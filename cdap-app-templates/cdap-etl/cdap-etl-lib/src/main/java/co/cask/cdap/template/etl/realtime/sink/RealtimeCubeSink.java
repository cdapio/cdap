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

package co.cask.cdap.template.etl.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordToCubeFact;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link RealtimeSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link RealtimeCubeSink} takes a {@link co.cask.cdap.api.data.format.StructuredRecord} in, maps it to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact}, and writes it to a {@link Cube} dataset identified by the
 * {@link co.cask.cdap.template.etl.common.Properties.Cube#DATASET_NAME} property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 * To configure transformation from {@link co.cask.cdap.api.data.format.StructuredRecord} to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} the
 * mapping configuration is required, as per {@link StructuredRecordToCubeFact} documentation.
 */
@Plugin(type = "sink")
@Name("Cube")
@Description("CDAP Cube Dataset Realtime Sink")
public class RealtimeCubeSink extends RealtimeSink<StructuredRecord> {
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private static final String NAME_PROPERTY_DESC = "Name of the Cube dataset. If the Cube does not already exist, " +
    "one will be created.";
  private static final String PROPERTY_RESOLUTIONS_DESC = "Aggregation resolutions to be used if new Cube dataset " +
    "needs to be created. See Cube dataset configuration " +
    "details for more information : http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/" +
    "cube.html#cube-configuration";
  private static final String DATASET_OTHER_PROPERTIES_DESC = "Provide any dataset properties to be used if new Cube " +
    "dataset needs to be created as a JSON Map. For example if aggregations are desired on fields - abc and xyz, the " +
    "property should have the value: " +
    "\"{\"dataset.cube.aggregation.agg1.dimensions\":\"abc\", \"dataset.cube.aggregation.agg2.dimensions\":\"xyz\"}." +
    " See Cube dataset configuration " +
    "details for more information : http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/" +
    "cube.html#cube-configuration";
  private static final String FACT_TS_FIELD_PROPERTY_DESC = "Name of the StructuredRecord's field that contains" +
    " timestamp to be used in CubeFact. If not provided, the current time of the record processing will be used as " +
    "CubeFact timestamp.";
  private static final String FACT_TS_FORMAT_PROPERTY_DESC = "Format of the value of timstamp field, " +
    "e.g. \"HH:mm:ss\" (used if " + Properties.Cube.FACT_TS_FIELD + " is provided).";
  private static final String MEASUREMENTS_PROPERTY_DESC = "Measurements to be extracted from StructuredRecord to be" +
    " used in CubeFact. Provide properties as a JSON Map. For example use 'price' field as a measurement of type" +
    " gauge, and 'count' field as a measurement of type counter, the property should have the value: " +
    "\"{\"cubeFact.measurement.price\":\"GAUGE\", \"cubeFact.measurement.count\":\"COUNTER\"}.";

  /**
   * Config class for RealtimeCubeSink
   */
  public static class RealtimeCubeSinkConfig extends PluginConfig {
    @Description(NAME_PROPERTY_DESC)
    String name;

    @Name(Properties.Cube.DATASET_RESOLUTIONS)
    @Description(PROPERTY_RESOLUTIONS_DESC)
    @Nullable
    String resolutions;

    @Name(Properties.Cube.DATASET_OTHER)
    @Description(DATASET_OTHER_PROPERTIES_DESC)
    @Nullable
    String datasetOther;

    @Name(Properties.Cube.FACT_TS_FIELD)
    @Description(FACT_TS_FIELD_PROPERTY_DESC)
    @Nullable
    String tsField;

    @Name(Properties.Cube.FACT_TS_FORMAT)
    @Description(FACT_TS_FORMAT_PROPERTY_DESC)
    @Nullable
    String tsFormat;

    @Name(Properties.Cube.MEASUREMENTS)
    @Description(MEASUREMENTS_PROPERTY_DESC)
    @Nullable
    String measurements;

    public RealtimeCubeSinkConfig(String name, String resolutions, String datasetOther,
                                  String tsField, String tsFormat, String measurements) {
      this.name = name;
      this.resolutions = resolutions;
      this.datasetOther = datasetOther;
      this.tsField = tsField;
      this.tsFormat = tsFormat;
      this.measurements = measurements;
    }
  }

  private final RealtimeCubeSinkConfig config;

  public RealtimeCubeSink(RealtimeCubeSinkConfig config) {
    this.config = config;
  }

  private StructuredRecordToCubeFact transform;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String datasetName = config.name;
    Map<String, String> properties = new HashMap<>(config.getProperties().getProperties());
    // todo: workaround for CDAP-2944: allows specifying custom props via JSON map in a property value
    if (!Strings.isNullOrEmpty(config.datasetOther)) {
      properties.remove(Properties.Cube.DATASET_OTHER);
      Map<String, String> datasetOther = GSON.fromJson(config.datasetOther, STRING_MAP_TYPE);
      properties.putAll(datasetOther);
    }
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");

    pipelineConfigurer.createDataset(datasetName, Cube.class.getName(), DatasetProperties.builder()
      .addAll(properties)
      .build());
  }

  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception {
    Cube cube = dataWriter.getDataset(config.name);
    int count = 0;
    for (StructuredRecord record : objects) {
      cube.add(transform.transform(record));
      count++;
    }
    return count;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    Map<String, String> runtimeArguments = config.getProperties().getProperties();
    transform = new StructuredRecordToCubeFact(runtimeArguments);
  }
}
