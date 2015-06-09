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

package co.cask.cdap.template.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordToCubeFact;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link BatchCubeSink} takes a {@link StructuredRecord} in, maps it to a {@link CubeFact} using the mapping
 * configuration provided with the {@link co.cask.cdap.template.etl.common.Properties.Cube#MAPPING_CONFIG_PROPERTY}
 * property, and writes it to the {@link Cube} dataset identified by the name property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 * To configure transformation from {@link StructuredRecord} to a {@link CubeFact} the
 * mapping configuration is required, as per {@link StructuredRecordToCubeFact} documentation.
 */
// todo: add unit-test once CDAP-2156 is resolved
@Plugin(type = "sink")
@Name("Cube")
@Description("CDAP Cube Dataset Batch Sink")
public class BatchCubeSink extends BatchWritableSink<StructuredRecord, byte[], CubeFact> {
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
   * Config class for BatchCube
   */
  public static class BatchCubeConfig extends PluginConfig {
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

    public BatchCubeConfig(String name, String resProp, String configAsString, String customProperties) {
      this.name = name;
      this.resProp = resProp;
      this.configAsString = configAsString;
      this.customProperties = customProperties;
    }
  }

  private final BatchCubeConfig batchCubeConfig;

  public BatchCubeSink(BatchCubeConfig realtimeCubeConfig) {
    this.batchCubeConfig = realtimeCubeConfig;
  }

  private StructuredRecordToCubeFact transform;

  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    transform = new StructuredRecordToCubeFact(context.getPluginProperties().getProperties());
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = Maps.newHashMap(batchCubeConfig.getProperties().getProperties());
    if (!Strings.isNullOrEmpty(batchCubeConfig.customProperties)) {
      properties.remove(Properties.Cube.CUSTOM_PROPERTIES);
      Map<String, String> customProperties = GSON.fromJson(batchCubeConfig.customProperties, STRING_MAP_TYPE);
      properties.putAll(customProperties);
    }

    properties.put(Properties.BatchReadableWritable.NAME, batchCubeConfig.name);
    properties.put(Properties.BatchReadableWritable.TYPE, Cube.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], CubeFact>> emitter) throws Exception {
    emitter.emit(new KeyValue<byte[], CubeFact>(null, transform.transform(input)));
  }
}
