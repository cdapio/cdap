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

import co.cask.cdap.api.dataset.lib.cube.Cube;
import co.cask.cdap.api.dataset.lib.cube.CubeFact;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSinkWriter;
import co.cask.cdap.templates.etl.api.config.ETLStage;

/**
 * A {@link co.cask.cdap.templates.etl.api.batch.BatchSink} that writes data to a {@link Cube} dataset.
 */
// todo: add unit-test once CDAP-2156 is resolved
public class CubeSink extends BatchWritableSink<CubeFact, byte[], CubeFact> {

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("CubeSink");
    configurer.setDescription("CDAP Cube Dataset Batch Sink");
    configurer.addProperty(new Property(NAME, "Name of the Cube dataset. If the Cube does not already exist," +
      " one will be created.", true));
    configurer.addProperty(new Property(Cube.PROPERTY_RESOLUTIONS,
                                        "Aggregation resolutions. See Cube dataset " +
                                          "configuration details for more information", true));
  }

  @Override
  protected String getDatasetType(ETLStage config) {
    return Cube.class.getName();
  }

  @Override
  public void write(CubeFact input, BatchSinkWriter<byte[], CubeFact> writer) throws Exception {
    writer.write(null, input);
  }
}
