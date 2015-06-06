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
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link RealtimeSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link RealtimeCubeSink} takes a {@link co.cask.cdap.api.data.format.StructuredRecord} in, maps it to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} using the mapping configuration
 * provided with the {@link co.cask.cdap.template.etl.common.Properties.Cube#MAPPING_CONFIG_PROPERTY} property,
 * and writes it to a {@link Cube} dataset identified by the 
 * {@link co.cask.cdap.template.etl.common.Properties.Cube#NAME} property.
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
  private static final String PROPERTY_RESOLUTIONS_DESC = "Aggregation resolutions. See Cube dataset configuration " +
    "details for more information : http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/" +
    "cube.html#cube-configuration";
  private static final String MAPPING_CONFIG_PROPERTY_DESC = "The StructuredRecord to CubeFact mapping " +
    "configuration. More info on the format of this configuration can be found at http://docs.cask.co/cdap/current/" +
    "en/reference-manual/javadocs/co/cask/cdap/template/etl/common/StructuredRecordToCubeFact.html";

  private static final String CUSTOM_PROPERTIES_DESC = "Provide any custom properties (such as Aggregations) as a " +
    "JSON Map. For example if aggregations are desired on fields - abc and xyz, the property should have the value : " +
    "\"{\"dataset.cube.aggregation.agg1.dimensions\":\"abc\", \"dataset.cube.aggregation.agg2.dimensions\":\"xyz\"}";

  /**
   * Config class for RealtimeCube
   */
  public static class RealtimeCubeConfig extends PluginConfig {
    @Description(NAME_PROPERTY_DESC)
    String name;

    @Name(Properties.Cube.PROPERTY_RESOLUTIONS)
    @Description(PROPERTY_RESOLUTIONS_DESC)
    String resProp;

    @Name(Properties.Cube.MAPPING_CONFIG_PROPERTY)
    @Description(MAPPING_CONFIG_PROPERTY_DESC)
    String configAsString;

    @Name(Properties.Cube.CUSTOM_PROPERTIES)
    @Description(CUSTOM_PROPERTIES_DESC)
    @Nullable
    String customProperties;

    public RealtimeCubeConfig(String name, String resProp, String configAsString) {
      this.name = name;
      this.resProp = resProp;
      this.configAsString = configAsString;
    }
  }

  private final RealtimeCubeConfig realtimeCubeConfig;

  public RealtimeCubeSink(RealtimeCubeConfig realtimeCubeConfig) {
    this.realtimeCubeConfig = realtimeCubeConfig;
  }

  private StructuredRecordToCubeFact transform;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    String datasetName = realtimeCubeConfig.name;
    Map<String, String> properties = realtimeCubeConfig.getProperties().getProperties();
    if (!Strings.isNullOrEmpty(realtimeCubeConfig.customProperties)) {
      properties.remove(Properties.Cube.CUSTOM_PROPERTIES);
      Map<String, String> customProperties = GSON.fromJson(realtimeCubeConfig.customProperties, STRING_MAP_TYPE);
      properties.putAll(customProperties);
    }
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");
    pipelineConfigurer.createDataset(datasetName, Cube.class.getName(), DatasetProperties.builder()
      .addAll(properties)
      .build());
  }

  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception {
    Cube cube = dataWriter.getDataset(realtimeCubeConfig.name);
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
    Map<String, String> runtimeArguments = realtimeCubeConfig.getProperties().getProperties();
    transform = new StructuredRecordToCubeFact(runtimeArguments);
  }
}
