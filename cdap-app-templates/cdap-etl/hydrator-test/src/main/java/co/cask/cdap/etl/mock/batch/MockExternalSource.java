/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock source that can be used to write a list of records in a Table and reads them out in a pipeline run.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(MockExternalSource.PLUGIN_NAME)
public class MockExternalSource extends BatchSource<LongWritable, Text, StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String PLUGIN_NAME = "MockExternalSource";
  private static final Gson GSON = new Gson();
  private final Config config;

  public MockExternalSource(Config config) {
    this.config = config;
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    private String name;
    private String dirName;
  }

  @Override
  public void transform(KeyValue<LongWritable, Text> input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(GSON.fromJson(input.getValue().toString(), StructuredRecord.class));
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.of(config.name, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return TextInputFormat.class.getCanonicalName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return ImmutableMap.of(TextInputFormat.INPUT_DIR, config.dirName);
      }
    }));
  }

  public static ETLPlugin getPlugin(String name, String dirName) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", name);
    properties.put("dirName", dirName);
    return new ETLPlugin(PLUGIN_NAME, BatchSource.PLUGIN_TYPE, properties, null);
  }

  /**
   * Used to write the input records for the pipeline run. Should be called after the pipeline has been created.
   *
   * @param fileName file to write the records into
   * @param records records that should be the input for the pipeline
   */
  public static void writeInput(String fileName,
                                Iterable<StructuredRecord> records) throws Exception {
    String output = Joiner.on("\n").join(Iterables.transform(records,
                                                             new Function<StructuredRecord, String>() {
                                                               @Override
                                                               public String apply(StructuredRecord input) {
                                                                 return GSON.toJson(input);
                                                               }
                                                             })
    );
    Files.write(output, new File(fileName), Charsets.UTF_8);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true));
    properties.put("dirName", new PluginPropertyField("dirName", "", "string", true));
    return new PluginClass(BatchSource.PLUGIN_TYPE, PLUGIN_NAME, "", MockExternalSource.class.getName(),
                           "config", properties);
  }
}
