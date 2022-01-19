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

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.plugin.InvalidPluginConfigException;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.etl.proto.v2.validation.StageSchema;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationRequest;
import io.cdap.cdap.etl.proto.v2.validation.StageValidationResponse;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Tests for ValidationUtils
 */
public class ValidationUtilsTest {

  @Test
  public void testValidateNoException() {
    Schema testSchema = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin("testtable"));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(),
                validationRequest, getPluginConfigurer(MockSource.PLUGIN_CLASS), properties -> properties);
    Assert.assertTrue(stageValidationResponse.getFailures().isEmpty());
  }

  @Test
  public void testValidateException() {
    Schema testSchema = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin(null));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(),
                validationRequest, getPluginConfigurer(MockSource.PLUGIN_CLASS), properties -> properties);
    Assert.assertEquals(1, stageValidationResponse.getFailures().size());
  }

  @Test
  public void testMacroSubstitution() {
    Schema testSchema = Schema.recordOf("a", Schema.Field.of("a", Schema.of(Schema.Type.STRING)));
    StageSchema testStageSchema = new StageSchema("source", testSchema);
    ETLStage etlStage = new ETLStage("source", MockSource.getPlugin("@{tName}"));
    StageValidationRequest validationRequest = new StageValidationRequest(
      etlStage, Collections
      .singletonList(testStageSchema), false);
    String testtable = "testtable";
    StageValidationResponse stageValidationResponse = ValidationUtils
      .validate(NamespaceId.DEFAULT.getNamespace(), validationRequest,
                getPluginConfigurer(MockSource.PLUGIN_CLASS), properties -> {
        Map<String, String> propertiesCopy = new HashMap<>(properties);
        if (propertiesCopy.getOrDefault("tableName", "").equals("@{tName}")) {
          propertiesCopy.put("tableName", testtable);
        }
        return propertiesCopy;
      });
    Assert.assertTrue(stageValidationResponse.getFailures().isEmpty());
    Assert.assertEquals(testtable, stageValidationResponse.getSpec().getPlugin().getProperties().get("tableName"));
  }

  //Mock PluginConfigurer
  private PluginConfigurer getPluginConfigurer(PluginClass pluginClass) {
    return new PluginConfigurer() {
      @Override
      public <T> T usePlugin(String pluginType, String pluginName, String pluginId, PluginProperties properties,
                             PluginSelector selector) {
        String tableName = properties.getProperties().get("tableName");
        if (tableName == null || tableName.isEmpty()) {
          throw new InvalidPluginConfigException(pluginClass, Collections.singleton("tableName"), new HashSet<>());
        }
        MockSource.Config config = new MockSource.Config();
        MockSource.ConnectionConfig connectionConfig = new MockSource.ConnectionConfig();
        String schema = properties.getProperties().get("schema");
        String sleep = properties.getProperties().get("sleepInMillis");
        connectionConfig.setTableName(tableName);
        config.setConfig(connectionConfig, schema, null, sleep == null ? null : Long.parseLong(sleep));
        return (T) new MockSource(config);
      }

      @Override
      public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                         PluginProperties properties, PluginSelector selector) {
        return null;
      }

      @Override
      public Map<String, String> evaluateMacros(Map<String, String> properties,
                                                MacroEvaluator evaluator, MacroParserOptions options) throws
        InvalidMacroException {
        return null;
      }
    };
  }
}
