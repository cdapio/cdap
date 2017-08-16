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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.internal.app.runtime.batch.BasicOutputFormatProvider;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.FilenameFilter;
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
  private static final Gson GSON = new Gson();
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
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    OutputFormatProvider outputFormatProvider =
      new BasicOutputFormatProvider(TextOutputFormat.class.getCanonicalName(),
                                    ImmutableMap.of(TextOutputFormat.OUTDIR, config.dirName));

    if (config.name != null) {
      Output output = Output.of(config.name, outputFormatProvider);
      output.alias(config.alias);
      context.addOutput(output);
    } else {
      context.addOutput(config.alias, outputFormatProvider);
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, String>> emitter)
    throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), GSON.toJson(input)));
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
   * Used to read the records written by this sink.
   *
   * @param dirName directory where output files are found
   */
  public static List<StructuredRecord> readOutput(String dirName) throws Exception {
    File dir = new File(dirName);
    File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("part");
      }
    });
    if (files == null) {
      return Collections.emptyList();
    }

    List<StructuredRecord> records = new ArrayList<>();
    for (File file : files) {
      records.addAll(Lists.transform(Files.readLines(file, Charsets.UTF_8),
                                     new Function<String, StructuredRecord>() {
                                       @Override
                                       public StructuredRecord apply(String input) {
                                         return GSON.fromJson(input, StructuredRecord.class);
                                       }
                                     }));
    }
    return records;
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", false, false));
    properties.put("alias", new PluginPropertyField("alias", "", "string", true, false));
    properties.put("dirName", new PluginPropertyField("dirName", "", "string", true, false));
    return new PluginClass(BatchSink.PLUGIN_TYPE, PLUGIN_NAME, "",
                           MockExternalSink.class.getName(), "config", properties);
  }
}
