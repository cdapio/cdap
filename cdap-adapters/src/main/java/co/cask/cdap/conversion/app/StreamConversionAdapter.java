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

package co.cask.cdap.conversion.app;

import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.schedule.Schedules;
import co.cask.cdap.api.templates.AdapterConfigurer;
import co.cask.cdap.api.templates.ApplicationTemplate;
import com.google.gson.Gson;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;

/**
 * Application that converts a stream into a partitioned file set.
 */
public class StreamConversionAdapter extends ApplicationTemplate<AdapterArgs> {
  static final String CONFIG_KEY = "adapter.args";

  @Override
  public void configure() {
    setDescription("Periodically reads stream events and writes them to a time partitioned fileset");
    addMapReduce(new StreamConversionMapReduce());
    addWorkflow(new StreamConversionWorkflow());
  }

  @Override
  public void configureAdapter(String adapterName, AdapterArgs args,
                               AdapterConfigurer configurer) throws Exception {
    configurer.addRuntimeArgument(CONFIG_KEY, new Gson().toJson(args));
    configurer.setSchedule(Schedules.createTimeSchedule("test", "adapter schedule", "* * * * *"));

    ConversionConfig config = args.getConfig();
    configurer.createDataset(config.getSinkName(), "timePartitionedFileSet", FileSetProperties.builder()
      .setBasePath(config.getSinkName())
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", config.getSinkSchema().toString())
      .build());
  }

}
