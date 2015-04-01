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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.api.templates.ApplicationTemplate;
import co.cask.cdap.api.templates.ManifestConfigurer;
import co.cask.cdap.internal.schedule.TimeSchedule;
import co.cask.cdap.templates.etl.api.Transform;
import co.cask.cdap.templates.etl.api.batch.BatchSink;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.batch.sinks.KVTableSink;
import co.cask.cdap.templates.etl.batch.sources.KVTableSource;
import co.cask.cdap.templates.etl.common.Constants;
import co.cask.cdap.templates.etl.common.DefaultStageConfigurer;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * ETL Batch Template.
 */
public class ETLBatchTemplate extends ApplicationTemplate<JsonObject> {
  private static final Gson GSON = new Gson();
  private final Table<String, String, String> nameToClass;

  public ETLBatchTemplate() throws Exception {
    nameToClass = HashBasedTable.create();
    //TODO: Add classes from Lib here to be available for use in the ETL Adapter. Remove this when
    //plugins management is completed.
    initTable(Lists.<Class>newArrayList(KVTableSource.class, KVTableSink.class));
  }

  private void initTable(List<Class> classList) throws Exception {
    for (Class klass : classList) {
      DefaultStageConfigurer configurer = new DefaultStageConfigurer(klass);
      if (BatchSource.class.isAssignableFrom(klass)) {
        BatchSource source = (BatchSource) klass.newInstance();
        source.configure(configurer);
        nameToClass.put("source", configurer.createSpecification().getName(),
                        configurer.createSpecification().getClassName());
      } else if (BatchSink.class.isAssignableFrom(klass)) {
        BatchSink sink = (BatchSink) klass.newInstance();
        sink.configure(configurer);
        nameToClass.put("sink", configurer.createSpecification().getName(),
                        configurer.createSpecification().getClassName());
      } else {
        Preconditions.checkArgument(Transform.class.isAssignableFrom(klass));
        Transform transform = (Transform) klass.newInstance();
        transform.configure(configurer);
        nameToClass.put("transform", configurer.createSpecification().getName(),
                        configurer.createSpecification().getClassName());
      }
    }
  }

  @Override
  public void configureManifest(JsonObject adapterConfig, ManifestConfigurer configurer) throws Exception {
    // Get name of the Adapter
    String adapterName = adapterConfig.get(Constants.ADAPTER_NAME).getAsString();

    JsonObject etlConfig = adapterConfig.get(Constants.CONFIG_KEY).getAsJsonObject();

    // Get cronEntry string for ETL Batch Adapter
    String cronEntry = etlConfig.get(Constants.SCHEDULE_KEY).getAsString();

    JsonObject source = etlConfig.get(Constants.SOURCE_KEY).getAsJsonObject();
    JsonObject sink = etlConfig.get(Constants.SINK_KEY).getAsJsonObject();
    JsonArray transform = etlConfig.get(Constants.TRANSFORM_KEY).getAsJsonArray();

    configureSource(source, configurer);
    configureSink(sink, configurer);
    configureTransform(transform, configurer);

    //TODO: Validate if source, transforms, sink can be tied together

    configurer.setSchedule(new TimeSchedule(String.format("etl.batch.adapter.%s.schedule", adapterName),
                                            String.format("Schedule for %s Adapter", adapterName),
                                            cronEntry));
  }

  private void configureSource(JsonObject source, ManifestConfigurer configurer) {
    String sourceName = source.get(Constants.Source.NAME).getAsString();
    configurer.addRuntimeArgument(Constants.Source.CLASS_NAME, nameToClass.get("source", sourceName));
  }

  private void configureSink(JsonObject sink, ManifestConfigurer configurer) {
    String sinkName = sink.get(Constants.Sink.NAME).getAsString();
    configurer.addRuntimeArgument(Constants.Sink.CLASS_NAME, nameToClass.get("sink", sinkName));
  }

  private void configureTransform(JsonArray transformArray, ManifestConfigurer configurer) {
    List<String> transformClasses = Lists.newArrayList();
    for (JsonElement transform : transformArray) {
      JsonObject transformObject = transform.getAsJsonObject();
      String transformName = transformObject.get(Constants.Transform.NAME).getAsString();
      transformClasses.add(nameToClass.get("transform", transformName));
    }
    configurer.addRuntimeArgument(Constants.Transform.TRANSFORM_CLASS_LIST, GSON.toJson(transformClasses));
  }

  @Override
  public void configure() {
    setName("etlbatch");
    setDescription("Batch Extract-Transform-Load (ETL) Adapter");
    addMapReduce(new ETLMapReduce());
    addWorkflow(new ETLWorkflow());
  }
}
