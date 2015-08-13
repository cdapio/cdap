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
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.batch.BatchSink;
import co.cask.cdap.template.etl.api.batch.BatchSinkContext;
import co.cask.cdap.template.etl.common.CubeSinkConfig;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordToCubeFact;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BatchSink} that writes data to a {@link Cube} dataset.
 * <p/>
 * This {@link BatchCubeSink} takes a {@link StructuredRecord} in, maps it to a {@link CubeFact},
 * and writes it to the {@link Cube} dataset identified by the name property.
 * <p/>
 * If {@link Cube} dataset does not exist, it will be created using properties provided with this sink. See more
 * information on available {@link Cube} dataset configuration properties at
 * {@link co.cask.cdap.data2.dataset2.lib.cube.CubeDatasetDefinition}.
 * <p/>
 * To configure transformation from {@link StructuredRecord} to a {@link CubeFact} the
 * mapping configuration is required, following {@link StructuredRecordToCubeFact} documentation.
 */
// todo: add unit-test once CDAP-2156 is resolved
@Plugin(type = "sink")
@Name("Cube")
@Description("CDAP Cube Dataset Batch Sink")
public class BatchCubeSink extends BatchWritableSink<StructuredRecord, byte[], CubeFact> {
  private static final Gson GSON = new Gson();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final CubeSinkConfig config;

  public BatchCubeSink(CubeSinkConfig config) {
    this.config = config;
  }

  private StructuredRecordToCubeFact transform;

  @Override
  public void initialize(BatchSinkContext context) throws Exception {
    super.initialize(context);
    transform = new StructuredRecordToCubeFact(context.getPluginProperties().getProperties());
  }

  @Override
  protected Map<String, String> getProperties() {
    Map<String, String> properties = new HashMap<>(config.getProperties().getProperties());
    // todo: workaround for CDAP-2944: allows specifying custom props via JSON map in a property value
    if (!Strings.isNullOrEmpty(config.getDatasetOther())) {
      properties.remove(Properties.Cube.DATASET_OTHER);
      Map<String, String> customProperties = GSON.fromJson(config.getDatasetOther(), STRING_MAP_TYPE);
      properties.putAll(customProperties);
    }
    properties.put(Properties.BatchReadableWritable.NAME, config.getName());
    properties.put(Properties.BatchReadableWritable.TYPE, Cube.class.getName());

    // will invoke validation of properties for mapping to the CubeFact
    new StructuredRecordToCubeFact(properties);

    return properties;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<byte[], CubeFact>> emitter) throws Exception {
    emitter.emit(new KeyValue<byte[], CubeFact>(null, transform.transform(input)));
  }
}
