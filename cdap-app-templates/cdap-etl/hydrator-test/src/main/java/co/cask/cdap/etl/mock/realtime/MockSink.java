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

package co.cask.cdap.etl.mock.realtime;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.plugin.PluginPropertyField;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Mock sink to keep track of what was written. For every call to write, it will write its records to a new file.
 */
@Plugin(type = RealtimeSink.PLUGIN_TYPE)
@Name("Mock")
public class MockSink extends RealtimeSink<StructuredRecord> {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(StructuredRecord.class, new StructuredRecordCodec())
    .create();
  private static final Type LIST_TYPE = new TypeToken<List<StructuredRecord>>() { }.getType();
  private final Config config;
  private int count;
  private File dir;

  public MockSink(Config config) {
    this.config = config;
    this.count = 0;
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    if (config.dir == null) {
      return;
    }

    this.dir = new File(config.dir);
    if (!this.dir.exists()) {
      throw new IllegalArgumentException(config.dir + " does not exist.");
    }
    if (!this.dir.isDirectory()) {
      throw new IllegalArgumentException(config.dir + " is not a directory");
    }
  }

  @Override
  public int write(Iterable<StructuredRecord> records, DataWriter dataWriter) throws Exception {
    if (dir == null) {
      return 0;
    }
    File outputFile = new File(dir, String.valueOf(count));
    File doneFile = new File(dir, String.valueOf(count) + ".done");

    List<StructuredRecord> outputRecords = new ArrayList<>();
    for (StructuredRecord record : records) {
      outputRecords.add(record);
    }
    Files.write(outputFile.toPath(), GSON.toJson(records).getBytes(StandardCharsets.UTF_8));
    doneFile.createNewFile();
    count++;
    return outputRecords.size();
  }

  /**
   * Config for the sink.
   */
  public static class Config extends PluginConfig {
    @Nullable
    private String dir;
  }

  // should pass in a temporary directory to ensure proper cleanup
  public static ETLPlugin getPlugin(File dir) {
    Map<String, String> properties = new HashMap<>();
    if (dir != null) {
      properties.put("dir", dir.getAbsolutePath());
    }
    return new ETLPlugin("Mock", RealtimeSink.PLUGIN_TYPE, properties, null);
  }

  /**
   * Get the records written out by the writeNum call to write. Waits until the results are available.
   *
   * @param dir the temporary directory that results are written to
   * @param writeNum the number of times write was called
   * @param timeout how long to wait
   * @param timeUnit time unit for how long to wait
   * @return results for the writeNum call to write.
   */
  public static List<StructuredRecord> getRecords(final File dir, final int writeNum,
                                                  long timeout, TimeUnit timeUnit) throws IOException,
    InterruptedException, ExecutionException, TimeoutException {


    Tasks.waitFor(true, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        File doneFile = new File(dir, writeNum + ".done");
        return doneFile.exists();
      }
    }, timeout, timeUnit);

    File recordsFile = new File(dir, String.valueOf(writeNum));
    String contents = new String(Files.readAllBytes(recordsFile.toPath()), StandardCharsets.UTF_8);
    return GSON.fromJson(contents, LIST_TYPE);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("dir", new PluginPropertyField("dir", "", "string", false));
    return new PluginClass(RealtimeSink.PLUGIN_TYPE, "Mock", "", MockSink.class.getName(), "config", properties);
  }
}
