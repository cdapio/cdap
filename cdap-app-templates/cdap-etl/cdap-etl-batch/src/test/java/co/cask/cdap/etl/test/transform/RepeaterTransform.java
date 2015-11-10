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

package co.cask.cdap.etl.test.transform;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.etl.batch.source.FileBatchSource;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by bhooshanmogal on 11/8/15.
 */
@Plugin(type = "transform")
@Name("Repeater")
@Description("The Repeater transform repeat a record a specified number of times based on a local properties file.")
public class RepeaterTransform extends Transform<StructuredRecord, StructuredRecord> {

  private final Config config;
  private final Properties properties;

  public RepeaterTransform(Config config) {
    this.config = config;
    this.properties = new Properties();
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    File propsFile = context.getLocalFile(config.propsFile);
    properties.load(new FileInputStream(propsFile));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    String body = input.get("body");
    int repeatCount = Integer.parseInt(properties.getProperty(body));
    StringBuilder repeated = new StringBuilder();
    for (int i = 0; i < repeatCount; i++) {
      repeated.append(body);
    }
    StructuredRecord output = StructuredRecord.builder(FileBatchSource.DEFAULT_SCHEMA)
      .set("ts", System.currentTimeMillis())
      .set("body", repeated.toString())
      .build();
    emitter.emit(output);
  }

  public static final class Config extends PluginConfig {
    @Name("propertiesFile")
    @Description("Name of the properties file to use in this transform.")
    String propsFile;
  }
}
