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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.batch.BatchSinkContext;
import co.cask.cdap.templates.etl.common.Properties;
import co.cask.cdap.templates.etl.common.StructuredRecordToCubeFact;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * A {@link co.cask.cdap.templates.etl.api.batch.BatchSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link BatchCubeSink} takes {@link StructuredRecord} in, maps it to a {@link CubeFact} using mapping
 * configuration provided with {@link Properties.Cube#MAPPING_CONFIG_PROPERTY} property, and writes it to a
 * {@link Cube} dataset identified by name property.
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
  private static final String NAME_PROPERTY_DESC = "Name of the Cube dataset. If the Cube does not already exist, " +
    "one will be created.";
  private static final String PROPERTY_RESOLUTIONS_DESC = "Aggregation resolutions. See Cube dataset configuration " +
    "details for more information";
  private static final String MAPPING_CONFIG_PROPERTY_DESC = "The StructuredRecord to CubeFact mapping configuration.";

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

    public BatchCubeConfig(String name, String resProp, String configAsString) {
      this.name = name;
      this.resProp = resProp;
      this.configAsString = configAsString;
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
    properties.put(Properties.BatchReadableWritable.NAME, batchCubeConfig.name);
    properties.put(Properties.BatchReadableWritable.TYPE, Cube.class.getName());
    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], CubeFact>> emitter) throws Exception {
    emitter.emit(new KeyValue<byte[], CubeFact>(null, transform.transform(input)));
  }
}
