/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.gson.Gson;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginPropertyField;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchContext;
import io.cdap.cdap.etl.api.engine.sql.BatchSQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngine;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLJoinRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock SQL engine that can be used to test join pipelines.
 */
@Plugin(type = BatchSQLEngine.PLUGIN_TYPE)
@Name(MockSQLEngine.NAME)
public class MockSQLEngine extends BatchSQLEngine<Object, Object, Object, Object>
  implements SQLEngine<Object, Object, Object, Object>, Serializable {
  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String NAME = "MockSQLEngine";
  private static final Gson GSON = new Gson();
  private final MockSQLEngine.Config config;
  boolean calledPrepareRun = false;
  boolean calledOnRunFinish = false;

  public MockSQLEngine(MockSQLEngine.Config config) {
    this.config = config;
  }

  /**
   * Config for the source.
   */
  public static class Config extends PluginConfig {
    private String name;
    private String inputDirName;
    private String outputDirName;
    private String outputSchema;
  }

  @Override
  public void prepareRun(RuntimeContext context) throws Exception {
    calledPrepareRun = true;
  }

  @Override
  public void onRunFinish(boolean succeeded, RuntimeContext context) {
    calledOnRunFinish = true;
  }

  @Override
  public SQLPushDataset<StructuredRecord, Object, Object> getPushProvider(SQLPushRequest pushRequest)
    throws SQLEngineException {
    if (!calledPrepareRun) {
      throw new SQLEngineException("prepareRun not called");
    }
    return new MockPushDataset(pushRequest, config.outputDirName);
  }

  @Override
  public SQLPullDataset<StructuredRecord, Object, Object> getPullProvider(SQLPullRequest pullRequest)
    throws SQLEngineException {
    if (!calledPrepareRun) {
      throw new SQLEngineException("prepareRun not called");
    }
    return new MockPullDataset(pullRequest, config.inputDirName);
  }

  @Override
  public boolean exists(String datasetName) throws SQLEngineException {
    return false;
  }

  @Override
  public boolean canJoin(SQLJoinRequest joinRequest) {
    return true;
  }

  @Override
  public SQLDataset join(SQLJoinRequest joinRequest) throws SQLEngineException {
    if (!calledPrepareRun) {
      throw new SQLEngineException("prepareRun not called");
    }
    return new SQLDataset() {
      @Override
      public String getDatasetName() {
        return "join";
      }

      @Override
      public Schema getSchema() {
        return GSON.fromJson(config.outputSchema, Schema.class);
      }

      @Override
      public long getNumRows() {
        return 1;
      }
    };
  }

  @Override
  public void cleanup(String datasetName) throws SQLEngineException {
    if (!calledPrepareRun) {
      throw new SQLEngineException("prepareRun not called");
    }
    if (!calledOnRunFinish) {
      throw new SQLEngineException("onRunFinish not called");
    }
  }

  public static ETLPlugin getPlugin(String name, String inputDirName, String outputDirName, Schema outputSchema) {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", name);
    properties.put("inputDirName", inputDirName);
    properties.put("outputDirName", outputDirName);
    properties.put("outputSchema", GSON.toJson(outputSchema));
    return new ETLPlugin(NAME, BatchSQLEngine.PLUGIN_TYPE, properties, null);
  }

  private static PluginClass getPluginClass() {
    Map<String, PluginPropertyField> properties = new HashMap<>();
    properties.put("name", new PluginPropertyField("name", "", "string", true, false));
    properties.put("inputDirName", new PluginPropertyField("inputDirName", "", "string", true, false));
    properties.put("outputDirName", new PluginPropertyField("outputDirName", "", "string", true, false));
    properties.put("outputSchema", new PluginPropertyField("outputSchema", "", "string", true, false));
    return new PluginClass(BatchSQLEngine.PLUGIN_TYPE, NAME, "", MockSQLEngine.class.getName(),
                           "config", properties);
  }

  /**
   * Used to write the input records for the pipeline run. Should be called after the pipeline has been created.
   *
   * @param fileName file to write the records into
   * @param records records that should be the input for the pipeline
   */
  public static void writeInput(String fileName,
                                Iterable<StructuredRecord> records) throws Exception {
    Function<StructuredRecord, String> mapper = input -> {
      try {
        return StructuredRecordStringConverter.toJsonString(input);
      } catch (IOException e) {
        throw new RuntimeException("Unable to set up file for test.", e);
      }
    };

    String output = Joiner.on("\n").join(Iterables.transform(records, mapper)
    );
    Files.write(output, new File(fileName), Charsets.UTF_8);
  }

  /**
   * Counts all lines in a directory used for Hadoop as output
   * @param directory File specitying the directory
   * @return
   * @throws IOException
   */
  public static int countLinesInDirectory(File directory) throws IOException {
    int lines = 0;

    return (int) java.nio.file.Files.walk(directory.toPath())
      .filter(java.nio.file.Files::isRegularFile)
      .filter(path -> !path.toString().endsWith(".crc") && !path.toString().endsWith("_SUCCESS")) // Filters some
      // hadoop files.
      .map(path -> {
        try {
          return Files.readLines(path.toFile(), Charsets.UTF_8);
        } catch (IOException e) {
          throw new RuntimeException("Unable to read file in directory", e);
        }
      })
      .flatMap(Collection::stream)
      .filter(l -> !l.isEmpty()) // Only consider not empty output files and lines.
      .count();
  }
}
