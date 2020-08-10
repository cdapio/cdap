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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Mock sink that writes records to a Table and has a utility method for getting all records written.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MockExternalSink.PLUGIN_NAME)
public class MockExternalSink extends BatchSink<StructuredRecord, NullWritable, String> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String PLUGIN_NAME = "MockExternalSink";
  private final Config config;

  public MockExternalSink(Config config) {
    this.config = config;
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    @Nullable
    private String name;
    private String alias;
    private String dirName;

    @Nullable
    private String name2;
    @Nullable
    private String alias2;
    @Nullable
    private String dirName2;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    OutputFormatProvider outputFormatProvider = new Provider(config.dirName);
    if (config.name != null) {
      Output output = Output.of(config.name, outputFormatProvider);
      output.alias(config.alias);
      context.addOutput(output);
    } else {
      context.addOutput(Output.of(config.alias, outputFormatProvider));
    }
    if (config.name2 != null) {
      context.addOutput(Output.of(config.name2, new Provider(config.dirName2)).alias(config.alias2));
    } else if (config.alias2 != null) {
      context.addOutput(Output.of(config.alias2, new Provider(config.dirName2)));
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, String>> emitter)
    throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), StructuredRecordStringConverter.toJsonString(input)));
  }

  /**
   * Output format provider that uses TextOutputFormat to write to a given directory.
   */
  public static class Provider implements OutputFormatProvider {
    private final String dirName;


    public Provider(String dirName) {
      this.dirName = dirName;
    }

    @Override
    public String getOutputFormatClassName() {
      return TextOutputFormat.class.getCanonicalName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return Collections.singletonMap(TextOutputFormat.OUTDIR, dirName);
    }
  }

  /**
   * Returns {@link ETLPlugin} for MockExternalSink.
   *
   * @param name name of the provider, use null for backwards compatibility
   * @param alias alias of the provider
   * @param dirName directory name
   * @return {@link ETLPlugin} for MockExternalSink
   */
  public static ETLPlugin getPlugin(@Nullable String name, String alias, String dirName) {
    Map<String, String> properties = new HashMap<>();
    if (name != null) {
      properties.put("name", name);
    }
    properties.put("alias", alias);
    properties.put("dirName", dirName);
    return new ETLPlugin(PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties, null);
  }


  /**
   * Returns {@link ETLPlugin} for MockExternalSink that writes the data to two different directories.
   */
  public static ETLPlugin getPlugin(String name1, String alias1, String dir1,
                                    String name2, String alias2, String dir2) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", name1);
    properties.put("alias", alias1);
    properties.put("dirName", dir1);
    properties.put("name2", name2);
    properties.put("alias2", alias2);
    properties.put("dirName2", dir2);
    return new ETLPlugin(PLUGIN_NAME, BatchSink.PLUGIN_TYPE, properties, null);
  }

  /**
   * Used to read the records written by this sink.
   *
   * @param dirName directory where output files are found
   */
  public static List<StructuredRecord> readOutput(String dirName, Schema schema) throws Exception {
    File dir = new File(dirName);
    File[] files = dir.listFiles((directory, name) -> name.startsWith("part"));
    if (files == null) {
      return Collections.emptyList();
    }

    List<StructuredRecord> records = new ArrayList<>();
    for (File file : files) {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        String line;
        while ((line = reader.readLine()) != null) {
          records.add(StructuredRecordStringConverter.fromJsonString(line, schema));
        }
      }
    }
    return records;
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", false, false));
    properties.put("alias", new PluginPropertyField("alias", "", "string", true, false));
    properties.put("dirName", new PluginPropertyField("dirName", "", "string", true, false));
    properties.put("name2", new PluginPropertyField("name2", "", "string", false, false));
    properties.put("alias2", new PluginPropertyField("alias2", "", "string", false, false));
    properties.put("dirName2", new PluginPropertyField("dirName2", "", "string", false, false));
    return new PluginClass(BatchSink.PLUGIN_TYPE, PLUGIN_NAME, "",
                           MockExternalSink.class.getName(), "config", properties);
  }
}
