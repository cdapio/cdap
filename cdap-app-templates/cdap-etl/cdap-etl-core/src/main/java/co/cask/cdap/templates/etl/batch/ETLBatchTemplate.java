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
import co.cask.cdap.templates.etl.common.Constants;
import com.google.common.collect.Lists;
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
    //TODO: Get the className given the sourceName
    String className = null;
    configurer.addRuntimeArgument(Constants.Source.CLASS_NAME, className);
  }

  private void configureSink(JsonObject sink, ManifestConfigurer configurer) {
    String sinkName = sink.get(Constants.Sink.NAME).getAsString();
    //TODO: Get the className given the sinkName
    String className = null;
    configurer.addRuntimeArgument(Constants.Sink.CLASS_NAME, className);
  }

  private void configureTransform(JsonArray transformArray, ManifestConfigurer configurer) {
    List<String> transformClasses = Lists.newArrayList();
    for (JsonElement transform : transformArray) {
      JsonObject transformObject = transform.getAsJsonObject();
      String transformName = transformObject.get(Constants.Transform.NAME).getAsString();
      //TODO: Get the className given the transformName
      String className = null;
      transformClasses.add(className);
    }
    configurer.addRuntimeArgument(Constants.Transform.TRANSFORM_CLASS_LIST, GSON.toJson(transformClasses));
  }

  @Override
  public void configure() {
    setName("etlbatch");
    setDescription("Batch Extract-Transform-Load (ETL) Adapter");
    addMapReduce(new BatchDriver());
    addWorkflow(new BatchWorkflow());
  }
}
