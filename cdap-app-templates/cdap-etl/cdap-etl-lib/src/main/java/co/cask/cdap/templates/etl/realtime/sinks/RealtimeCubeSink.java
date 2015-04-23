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

package co.cask.cdap.templates.etl.realtime.sinks;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.DataWriter;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSink;
import co.cask.cdap.templates.etl.common.StructuredRecordToCubeFact;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * A {@link co.cask.cdap.templates.etl.api.realtime.RealtimeSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link RealtimeCubeSink} takes in {@link co.cask.cdap.api.data.format.StructuredRecord} in, maps it to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} using mapping configuration
 * provided with {@link #MAPPING_CONFIG_PROPERTY} property, and writes it to a {@link Cube} dataset identified by
 * {@link #NAME_PROPERTY} property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 * To configure transformation from {@link co.cask.cdap.api.data.format.StructuredRecord} to a
 * {@link co.cask.cdap.api.dataset.lib.cube.CubeFact} the
 * mapping configuration is required, as per {@link StructuredRecordToCubeFact} documentation.
 */
// todo: add unit-test once CDAP-2156 is resolved
public class RealtimeCubeSink extends RealtimeSink<StructuredRecord> {
  public static final String NAME_PROPERTY = "name";
  public static final String MAPPING_CONFIG_PROPERTY = "mapping.config";

  private String datasetName;
  private StructuredRecordToCubeFact transform;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("RealtimeCubeSink");
    configurer.setDescription("CDAP Cube Dataset Realtime Sink");
    configurer.addProperty(new Property(NAME_PROPERTY, "Name of the Cube dataset. If the Cube does not already exist," +
      " one will be created.", true));
    configurer.addProperty(new Property(Cube.PROPERTY_RESOLUTIONS,
                                        "Aggregation resolutions. See Cube dataset " +
                                          "configuration details for more information", true));
    configurer.addProperties(StructuredRecordToCubeFact.getProperties());
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    String datasetName = stageConfig.getProperties().get(NAME_PROPERTY);
    Preconditions.checkArgument(datasetName != null && !datasetName.isEmpty(), "Dataset name must be given.");
    pipelineConfigurer.createDataset(datasetName, Cube.class.getName(), DatasetProperties.builder()
      .addAll(stageConfig.getProperties())
      .build());
  }

  @Override
  public int write(Iterable<StructuredRecord> objects, DataWriter dataWriter) throws Exception {
    Cube cube = dataWriter.getDataset(datasetName);
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
    Map<String, String> runtimeArguments = context.getRuntimeArguments();
    datasetName = runtimeArguments.get(NAME_PROPERTY);
    transform = new StructuredRecordToCubeFact(runtimeArguments);
  }
}
